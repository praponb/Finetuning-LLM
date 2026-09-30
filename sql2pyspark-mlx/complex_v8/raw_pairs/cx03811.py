from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.sum("days_since_event_at").alias("session_days_since_event_at"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

per_group = (
    per_session
    .groupBy("n_events")
    .agg(
        F.count(F.when(F.col("event_type") == "resolved", 1)).alias("n_event_type_resolved"),
        F.count("*").alias("n_rows"),
        F.count(F.when(F.col("event_type") == "reopened", 1)).alias("n_event_type_reopened"),
        F.round(100 * F.count(F.when(F.col("event_type").isin("escalated", "resolved", "reopened"), 1)) / F.count("*"), 1).alias("pct_event_type_in_escalated_reso"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.min("session_end").alias("first_session_end"),
    )
)

result = (
    per_group
    .select(
        "n_events",
        "n_event_type_resolved",
        "n_rows",
        "n_event_type_reopened",
        "pct_event_type_in_escalated_reso",
        "distinct_event_types",
        "first_session_end",
        F.abs(F.col("n_events") - 3).alias("n_events_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("first_session_end")).alias("days_since_first_session_end"),
    )
    .orderBy("first_session_end", F.col("n_event_type_reopened").asc_nulls_last())
)
