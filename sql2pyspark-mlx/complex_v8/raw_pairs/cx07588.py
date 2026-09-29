from pyspark.sql import functions as F

cleaned = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("reopened", "assigned"), "group_a").when(F.col("event_type") == "escalated", "group_b").otherwise("other").alias("event_type_group"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

with_stats = (
    cleaned.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

enriched_ticket = (
    with_stats.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.csat").alias("ticket_csat"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    enriched_ticket
    .rollup("ticket_csat", "actor", "event_type_group")
    .agg(
        F.grouping("ticket_csat").alias("is_total_ticket_csat"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("event_type_group").alias("distinct_event_type_groups"),
    )
)
