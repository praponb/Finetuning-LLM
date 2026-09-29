from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("opened_at") < F.lit("2025-02-24 14:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

with_prev = recent_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 45), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "burst_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    sessions
    .crossJoin(sessions.agg(F.max("burst_id").alias("overall_max_burst_id")))
    .filter(F.col("burst_id") < F.col("overall_max_burst_id"))
    .drop("overall_max_burst_id")
)

tagged = (
    with_activity
    .filter(F.col("duration_min") < 216.0)
    .select(
        "priority",
        "burst_id",
        "session_start",
        "session_end",
        "duration_min",
        F.least(F.col("burst_id"), F.col("event_count")).alias("least_burst_id_event_count"),
        F.lower(F.regexp_replace(F.trim(F.col("priority")), " ", "_")).alias("priority_slug"),
    )
)

result = (
    tagged
    .select(
        "priority",
        "burst_id",
        "session_end",
        "duration_min",
        "priority_slug",
        F.concat_ws("-", F.col("priority_slug"), F.col("priority")).alias("priority_slug_priority_label"),
        F.coalesce(F.col("burst_id"), F.lit(0)).alias("burst_id_filled"),
    )
    .orderBy(F.col("priority_slug_priority_label").asc_nulls_last())
)
