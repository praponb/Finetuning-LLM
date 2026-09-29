from pyspark.sql import functions as F

joined_ranges = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 3 DAYS"))),
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

per_group = (
    joined_ranges
    .groupBy("ticket_id", F.to_date("event_at").alias("event_at_day"))
    .agg(
        F.min("hours_since_open").alias("min_hours_since_open"),
        F.min("event_at").alias("first_event_at"),
        F.sum(F.when(F.col("event_type") == "replied", 1).otherwise(0)).alias("n_event_type_replied"),
        F.sum(F.when(F.col("priority").isNotNull(), F.col("hours_since_open"))).alias("hours_since_open_priority_is_not_null"),
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.max("hours_since_open").alias("max_hours_since_open_check"),
    )
    .filter(F.col("max_hours_since_open_check") > 14.4)
    .drop("max_hours_since_open_check")
)

scored = (
    per_group
    .filter(F.col("event_at_day") < F.lit("2025-05-01").cast("date"))
    .select(
        "*",
        F.when(F.col("total_hours_since_open") >= 324.0, "HIGH").when(F.col("total_hours_since_open") >= 108.0, "MEDIUM").otherwise("LOW").alias("total_hours_since_open_tier"),
        (F.col("first_event_at") + F.expr("INTERVAL 30 MINUTES")).alias("first_event_at_plus_30m"),
        F.round(F.col("total_hours_since_open") * 0.25, 2).alias("total_hours_since_open_adj"),
    )
)

result = (
    scored
    .filter(F.col("first_event_at") < F.lit("2025-02-24 10:00:00").cast("timestamp"))
    .select(
        "ticket_id",
        "event_at_day",
        "min_hours_since_open",
        "first_event_at",
        "n_event_type_replied",
        "hours_since_open_priority_is_not_null",
        "total_hours_since_open",
        "total_hours_since_open_tier",
        "total_hours_since_open_adj",
        F.coalesce(F.col("min_hours_since_open"), F.lit(0.0)).alias("min_hours_since_open_filled"),
    )
    .orderBy(
        F.col("hours_since_open_priority_is_not_null").desc(),
        F.col("n_event_type_replied").desc(),
    )
)
