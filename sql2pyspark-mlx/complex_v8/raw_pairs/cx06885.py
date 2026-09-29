from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("event_type").orderBy(F.col("event_event_at").desc(), F.col("id").desc(), F.col("event_id").desc())

filtered_events = (
    df_ticket_events
    .filter(F.col("actor").isin("system", "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.date_trunc("month", F.col("event_at")).alias("event_at_month"),
    )
)

line_level_events = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

deduped = (
    line_level_events
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    deduped
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_event_at")).alias("days_since_event_event_at"),
    )
)

result = (
    tagged
    .rollup("event_event_type", "ticket_id")
    .agg(
        F.grouping("event_event_type").alias("is_total_event_event_type"),
        F.sum(F.when(F.col("event_event_type").isin("replied", "escalated"), F.col("days_since_event_event_at")).otherwise(0)).alias("days_since_event_event_at_event_event_type_in_re"),
    )
    .select(
        F.coalesce(F.col("event_event_type"), F.lit("ALL")).alias("event_event_type"),
        "ticket_id",
        "is_total_event_event_type",
        "days_since_event_event_at_event_event_type_in_re",
    )
)
