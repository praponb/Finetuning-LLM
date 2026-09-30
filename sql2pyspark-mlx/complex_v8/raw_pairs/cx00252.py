from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id").orderBy("opened_at", "id")
w_agent_id_running = Window.partitionBy("agent_id").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        F.concat(F.col("channel"), F.lit("#"), F.col("id").cast("string")).alias("channel_ref"),
    )
)

with_prev = valid.withColumn("prev_opened_at", F.lag("opened_at").over(w_agent_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_agent_id_running))

session_summary = (
    sessionized
    .groupBy("agent_id", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

derived = (
    session_summary
    .filter(F.col("session_start") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.round(F.col("duration_min") / F.nullif(F.col("session_requester_id"), F.lit(0)), 3).alias("duration_min_per_session_requester_id"),
        F.date_trunc("hour", F.col("session_end")).alias("session_end_hour"),
        F.round(F.col("event_count") / F.nullif(F.col("burst_id"), F.lit(0)), 3).alias("event_count_per_burst_id"),
    )
)

result = (
    derived
    .filter(F.col("event_count_per_burst_id") >= 3.0)
    .select(
        "agent_id",
        "burst_id",
        "session_start",
        "session_end",
        "session_requester_id",
        "duration_min",
        "duration_min_per_session_requester_id",
        "event_count_per_burst_id",
    )
    .orderBy(F.col("session_end").desc(), "agent_id", "burst_id")
    .limit(5)
)
