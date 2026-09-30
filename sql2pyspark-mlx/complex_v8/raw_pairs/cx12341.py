from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

enriched = (
    sessions
    .select(
        "ticket_id",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        (F.floor(F.col("duration_min") / 50) * 50).alias("duration_min_bucket"),
        F.to_date(F.col("session_start")).alias("session_start_date"),
        F.round(F.col("duration_min") * 1.07, 2).alias("duration_min_adj"),
    )
)

per_group = (
    enriched
    .groupBy(
        "session_id",
        F.year("session_start_date").alias("session_start_date_year"),
        "ticket_id",
    )
    .agg(
        F.min("n_events").alias("min_n_events"),
        F.min("session_id").alias("min_session_id"),
        F.sum("duration_min_bucket").alias("total_duration_min_bucket"),
    )
)

result = (
    per_group
    .filter(F.col("session_start_date_year").between(2024, 2025))
    .select(
        "session_id",
        "session_start_date_year",
        "min_n_events",
        "min_session_id",
        "total_duration_min_bucket",
        F.coalesce(F.col("session_id"), F.lit(0)).alias("session_id_filled"),
    )
    .orderBy("min_session_id")
)
