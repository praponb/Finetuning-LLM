from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("opened_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .filter(F.array_contains(F.col("tags"), "refund"))
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

with_prev = filtered_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_opened_at").isNull() | ((F.unix_timestamp("opened_at") - F.unix_timestamp("prev_opened_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_priority_running))

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

annotated = (
    per_session
    .filter(F.col("session_id") <= 5)
    .select(
        "*",
        F.length(F.trim(F.col("priority"))).alias("priority_len"),
        F.when(F.col("event_count") >= 5, "large").when(F.col("event_count") >= 3, "medium").otherwise("small").alias("event_count_tier"),
        F.when(F.col("priority").isin("P4", "P3"), "group_a").when(F.col("priority") == "P1", "group_b").otherwise("other").alias("priority_group"),
    )
)

result = (
    annotated
    .select(
        "priority",
        "session_id",
        "session_end",
        "event_count",
        "duration_min",
        "priority_group",
        F.quarter(F.col("session_start")).alias("session_start_quarter"),
    )
    .orderBy("session_id", F.col("priority_group").desc())
)
