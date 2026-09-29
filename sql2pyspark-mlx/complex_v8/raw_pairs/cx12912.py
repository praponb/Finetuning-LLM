from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("closed_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start_ties = Window.orderBy(F.col("session_start").desc())
w_all_by_session_start = Window.orderBy(F.col("session_start").desc(), "agent_id", "session_no")
w_all_by_session_start_running = Window.orderBy(F.col("session_start").desc(), "agent_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_start_moving = Window.orderBy(F.col("session_start").desc(), "agent_id", "session_no").rowsBetween(-2, Window.currentRow)

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
    )
)

with_prev = scoped_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_agent_id_running))

sessions = (
    sessionized
    .groupBy("agent_id", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

with_windows = (
    sessions
    .select(
        "*",
        F.dense_rank().over(w_all_by_session_start_ties).alias("session_start_dense_rank"),
        F.row_number().over(w_all_by_session_start).alias("seq_no"),
        F.first("session_end", ignorenulls=True).over(w_all_by_session_start_running).alias("first_known_session_end"),
        F.ntile(5).over(w_all_by_session_start).alias("session_start_quintile"),
        F.round(F.avg("duration_min").over(w_all_by_session_start_moving), 2).alias("duration_min_avg_3"),
    )
)

result = (
    with_windows
    .filter(F.col("session_end") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "agent_id",
        "session_no",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "session_start_dense_rank",
        "first_known_session_end",
        "session_start_quintile",
    )
    .orderBy(F.col("session_end").desc())
)
