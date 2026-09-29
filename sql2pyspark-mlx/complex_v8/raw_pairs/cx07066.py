from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_tickets
    .filter(F.col("channel") == "web")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

with_prev = filtered.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_session_flag").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    sessions
    .filter(F.col("events").between(2, 6))
    .select(
        "priority",
        "session_end",
        "events",
        "session_requester_id",
        "duration_min",
        F.abs(F.col("events") - 4).alias("events_distance"),
        F.coalesce(F.col("events"), F.lit(0)).alias("events_filled"),
    )
    .orderBy(F.col("events").asc())
)
