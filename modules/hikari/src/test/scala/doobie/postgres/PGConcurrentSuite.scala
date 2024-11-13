// Copyright (c) 2013-2020 Rob Norris and Contributors
// This software is licensed under the MIT License (MIT).
// For more information see LICENSE or https://opensource.org/licenses/MIT

package doobie.postgres

import cats.effect.IO
import com.zaxxer.hikari.HikariDataSource
import doobie.*
import doobie.implicits.*
import fs2.Stream

import java.util.concurrent.Executors
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

class PGConcurrentSuite extends munit.FunSuite {

  import cats.effect.unsafe.implicits.global

  private var dataSource: HikariDataSource = _

  private def createDataSource() = {

    Class.forName("org.postgresql.Driver")
    val dataSource = new HikariDataSource

    dataSource setJdbcUrl "jdbc:postgresql://localhost:5432/postgres"
    dataSource setUsername "postgres"
    dataSource setPassword "password"
    dataSource setMaximumPoolSize 10
    dataSource setConnectionTimeout 2000
    dataSource
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataSource = createDataSource()
  }

  override def afterAll(): Unit = {
    dataSource.close()
    super.afterAll()
  }

  test("Not leak connections with recursive query streams") {

    val xa = Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(32))
    )

    val poll: fs2.Stream[IO, Int] =
      fr"select 1".query[Int].stream.transact(xa) ++ fs2.Stream.exec(IO.sleep(50.millis))

    val pollingStream: IO[Unit] = fs2.Stream.emits(List.fill(4)(poll.repeat))
      .parJoinUnbounded
      .take(20)
      .compile
      .drain

    assertEquals(pollingStream.unsafeRunSync(), ())
  }

  test("Connection returned before stream is drained") {
    val xa = Transactor.fromDataSource[IO](
      dataSource,
      ExecutionContext.fromExecutor(Executors.newFixedThreadPool(32))
    )

    val count = 100
    val insert = for {
      _ <- Stream.eval(sql"CREATE TABLE if not exists stream_cancel_test (i text)".update.run.transact(xa))
      _ <- Stream.eval(sql"truncate table stream_cancel_test".update.run.transact(xa))
      _ <- Stream.eval(sql"INSERT INTO stream_cancel_test values ('1')".update.run.transact(xa)).repeatN(count)
    } yield ()

    insert.compile.drain.unsafeRunSync()

    val streamLargerBuffer = fr"select * from stream_cancel_test".query[Int].stream.transact(xa)
      .evalMap(_ => fr"select 1".query[Int].unique.transact(xa))
      .compile.count

    assertEquals(streamLargerBuffer.unsafeRunSync(), count.toLong)

    // if buffer is less than result set, it will be still block new connection since the result set is not drained
    // use sleep to test the result set can be drained
    val streamSmallerBuffer = fr"select * from stream_cancel_test".query[Int].stream.transact(xa)
      .evalMap(_ => IO.sleep(10.milliseconds))
      .compile.count

    assertEquals(streamSmallerBuffer.unsafeRunSync(), count.toLong)
  }

}
