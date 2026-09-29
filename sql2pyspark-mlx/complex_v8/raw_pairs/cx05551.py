from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_requester_id_tier = Window.partitionBy("requester_id_tier").orderBy("opened_at", "id")
w_requester_id_tier_running = Window.partitionBy("requester_id_tier").orderBy("opened_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end_running = Window.orderBy(F.col("session_end").desc_nulls_first(), "requester_id_tier", "session_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end = Window.orderBy(F.col("session_end").desc_nulls_first(), "requester_id_tier", "session_id")

cleaned_tickets = (
    df_tickets
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
        F.when(F.col("requester_id") >= 17, "HIGH").when(F.col("requester_id") >= 8, "MEDIUM").otherwise("LOW").alias("requester_id_tier"),
    )
)

with_prev = cleaned_tickets.withColumn("prev_opened_at", F.lag("opened_at").over(w_requester_id_tier))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_opened_at").isNull() | (F.col("opened_at") > F.col("prev_opened_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_requester_id_tier_running))

per_session = (
    sessionized
    .groupBy("requester_id_tier", "session_id")
    .agg(
        F.min("opened_at").alias("session_start"),
        F.max("opened_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("requester_id").alias("session_requester_id"),
        F.round((F.unix_timestamp(F.max("opened_at")) - F.unix_timestamp(F.min("opened_at"))) / 60, 1).alias("duration_min"),
    )
)

labeled = (
    per_session
    .select(
        "*",
        F.substring(F.trim(F.col("requester_id_tier")), 1, 2).alias("requester_id_tier_prefix"),
        F.date_trunc("hour", F.col("session_end")).alias("session_end_hour"),
    )
)

ranked = (
    labeled
    .withColumn("first_known_event_count", F.first("event_count", ignorenulls=True).over(w_all_by_session_end_running))
    .withColumn("prev_session_end_hour", F.lag("session_end_hour").over(w_all_by_session_end))
)

result = (
    ranked
    .select(
        "requester_id_tier",
        "session_id",
        "session_start",
        "session_end",
        "event_count",
        "duration_min",
        "requester_id_tier_prefix",
        "session_end_hour",
        "prev_session_end_hour",
        F.coalesce(F.col("requester_id_tier"), F.lit("UNKNOWN")).alias("requester_id_tier_or_unknown"),
    )
)
