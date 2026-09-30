from pyspark.sql import functions as F

base = (
    df_ticket_events
    .filter((F.col("event_type").isin("created", "assigned")) & (F.col("event_at") < F.lit("2025-02-06 12:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

enriched = (
    base.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    enriched
    .groupingSets(
        [["ticket_priority", "actor"], ["ticket_priority"]],
        "ticket_priority", "actor",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("cnt"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .select(
        F.coalesce(F.col("ticket_priority"), F.lit("ALL")).alias("ticket_priority"),
        F.coalesce(F.col("actor"), F.lit("ALL")).alias("actor"),
        "grouping_level",
        "cnt",
        "distinct_ticket_ids",
    )
)
