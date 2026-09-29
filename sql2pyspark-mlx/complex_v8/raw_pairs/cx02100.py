from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority = Window.partitionBy("priority").orderBy("closed_at", "id")
w_priority_running = Window.partitionBy("priority").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        F.concat_ws(" / ", F.col("priority"), F.col("channel")).alias("priority_channel_label"),
    )
)

with_prev = filtered_tickets.withColumn("prev_closed_at", F.lag("closed_at").over(w_priority))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_closed_at").isNull() | (F.col("closed_at") > F.col("prev_closed_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("is_new_session").over(w_priority_running))

session_summary = (
    sessionized
    .groupBy("priority", "session_id")
    .agg(
        F.min("closed_at").alias("session_start"),
        F.max("closed_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("closed_at")) - F.unix_timestamp(F.min("closed_at"))) / 60, 1).alias("duration_min"),
    )
)

result = (
    session_summary
    .select(
        "priority",
        "session_id",
        "session_end",
        "event_count",
        "duration_min",
        F.round(F.col("duration_min") * 1.5, 2).alias("duration_min_adj"),
        F.when(F.col("event_count") >= 4, "large").when(F.col("event_count") >= 3, "medium").otherwise("small").alias("event_count_tier"),
    )
    .orderBy(F.col("duration_min_adj").desc())
)
