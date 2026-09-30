from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 39)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.count("*").alias("event_count"),
        F.min("event_at").alias("first_event_at"),
    )
)

profiled = (
    scoped.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.event_count"), F.lit(0)).alias("event_count"),
        "t.first_event_at",
    )
)

per_group = (
    profiled
    .groupBy("ticket_id", "actor")
    .agg(
        F.max("first_event_at").alias("last_first_event_at"),
        F.round(F.avg("event_count"), 2).alias("avg_event_count"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
        F.countDistinct("event_type").alias("distinct_event_types"),
        F.min("first_event_at").alias("first_first_event_at"),
    )
)

with_parent_ticket = (
    per_group.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.*",
        F.col("t.channel").alias("ticket_channel"),
        F.col("t.priority").alias("ticket_priority"),
    )
)

result = (
    with_parent_ticket
    .select(
        "ticket_id",
        "actor",
        F.col("distinct_event_types").alias("distinct_event_types_v"),
        F.col("distinct_ticket_ids").alias("m_distinct_ticket_ids"),
    )
    .unpivot(
        ids=["ticket_id", "actor"],
        values=["distinct_event_types_v", "m_distinct_ticket_ids"],
        variableColumnName="metric",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("ticket_id", "actor", "metric")
)
