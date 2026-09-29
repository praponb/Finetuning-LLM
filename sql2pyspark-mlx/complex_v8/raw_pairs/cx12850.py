from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_ticket_id_running))

session_summary = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

scored = (
    session_summary
    .select(
        "*",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("session_end")).alias("days_since_session_end"),
    )
)

result = (
    scored
    .select(
        "ticket_id",
        "burst_id",
        "session_start",
        "session_end",
        "events",
        "duration_min",
        "minutes_session_end_to_session_start",
        "days_since_session_end",
        F.when(F.col("days_since_session_end") >= 560, "HIGH").when(F.col("days_since_session_end") >= 315, "MEDIUM").otherwise("LOW").alias("days_since_session_end_tier"),
    )
    .orderBy(
        F.col("days_since_session_end_tier").desc_nulls_first(),
        F.col("session_start").asc_nulls_last(),
        "ticket_id",
        "burst_id",
    )
    .limit(20)
)
