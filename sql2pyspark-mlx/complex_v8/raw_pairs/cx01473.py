from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tickets = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp"))
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

with_prev = recent_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | ((F.unix_timestamp("closed_at") - F.unix_timestamp("prev_closed_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_priority_running))

sessions = (
    sessionized
    .groupBy("priority", "session_no")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    sessions
    .select(
        "priority",
        "session_no",
        "events",
        "duration_min",
        F.month(F.col("session_end")).alias("session_end_month"),
        (F.col("duration_min") + F.col("session_no")).alias("duration_min_plus_session_no"),
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
    )
)

result = (
    labeled
    .select(
        "priority",
        "session_no",
        "events",
        "duration_min",
        "session_end_month",
        "duration_min_plus_session_no",
        F.when(F.col("events") >= 4, "large").when(F.col("events") >= 2, "medium").otherwise("small").alias("events_tier"),
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
    )
    .orderBy(
        F.col("duration_min").asc_nulls_last(),
        F.col("session_no").asc(),
        "priority",
    )
    .limit(10)
)
