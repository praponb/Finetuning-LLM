from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 1 DAYS"))),
        "inner",
    )
    .select(
        "e.id",
        "e.ticket_id",
        "e.event_at",
        "e.event_type",
        "t.priority",
        "t.agent_id",
        F.round((F.unix_timestamp("e.event_at") - F.unix_timestamp("t.opened_at")) / 3600, 2).alias("hours_since_open"),
    )
)

exploded_detail = (
    joined_ranges.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

grouped = (
    exploded_detail
    .groupBy(
        "event_event_type",
        "event_id",
        F.to_date("event_event_at").alias("event_event_at_day"),
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("priority").isNotNull(), 1)) / F.count("*"), 1).alias("pct_priority_is_not_null"),
        F.countDistinct("event_event_type").alias("distinct_event_event_types"),
    )
)

result = (
    grouped
    .select(
        "event_event_type",
        "event_event_at_day",
        "pct_priority_is_not_null",
        "distinct_event_event_types",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_event_at_day")).alias("days_since_event_event_at_day"),
        F.abs(F.col("pct_priority_is_not_null") - 50.0).alias("pct_priority_is_not_null_distance"),
    )
    .orderBy(
        F.col("distinct_event_event_types").desc(),
        F.col("event_event_type").asc(),
    )
)
