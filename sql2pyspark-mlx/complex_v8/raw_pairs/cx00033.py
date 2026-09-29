from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.col("ticket_id").isNotNull()) & (F.to_date(F.col("event_at")) < F.lit("2025-02-12").cast("date")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.concat(F.col("actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("actor_ref"),
        F.length(F.trim(F.col("actor"))).alias("actor_len"),
    )
)

line_level = (
    scoped.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_at_label",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.count("*").alias("event_count"), F.count("*").alias("event_count_2"))
)

with_counts = (
    line_level.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        F.coalesce(F.col("t.event_count_2"), F.lit(0)).alias("event_count_2"),
    )
)

result = (
    with_counts
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "event_at_label",
        "event_id",
        "event_event_type",
        "event_count",
        "event_count_2",
        F.month(F.col("event_at")).alias("event_at_month"),
        F.when(F.col("event_count_2") >= 6, "large").when(F.col("event_count_2") >= 3, "medium").otherwise("small").alias("event_count_2_tier"),
    )
    .orderBy(F.col("event_count").desc())
)
