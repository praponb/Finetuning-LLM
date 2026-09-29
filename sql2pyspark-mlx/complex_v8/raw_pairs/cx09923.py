from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tickets
    .filter(F.col("requester_id") <= 17)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
    )
)

with_prev = recent.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_priority_running))

per_session = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    per_session
    .select(
        "priority",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        F.least(F.col("event_count"), F.col("duration_min")).alias("least_event_count_duration_min"),
        F.coalesce(F.col("duration_min"), F.lit(0.0)).alias("duration_min_filled"),
    )
    .orderBy("session_id", F.col("session_start").desc())
)
