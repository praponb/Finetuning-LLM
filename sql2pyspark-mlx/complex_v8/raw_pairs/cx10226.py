from pyspark.sql import functions as F

matched = (
    df_ticket_events.alias("e")
    .join(
        df_tickets.alias("t"),
        (F.col("t.id") == F.col("e.ticket_id"))
        & (F.col("e.event_at") >= F.col("t.opened_at"))
        & (F.col("e.event_at") <= F.coalesce(F.col("t.closed_at"), F.col("t.opened_at") + F.expr("INTERVAL 2 DAYS"))),
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

joined_ticket = (
    matched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.priority",
        "s.agent_id",
        "s.hours_since_open",
        F.col("t.channel").alias("ticket_channel"),
    )
)

ticket_channel_stats = (
    joined_ticket
    .groupBy("ticket_channel")
    .agg(F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"))
)

above_average = (
    joined_ticket.alias("s")
    .join(ticket_channel_stats.alias("g"), F.col("g.ticket_channel") == F.col("s.ticket_channel"), "inner")
    .filter(F.col("s.hours_since_open") >= F.col("g.median_hours_since_open"))
    .select(
        "s.*",
        "g.median_hours_since_open",
        F.round(F.col("s.hours_since_open") - F.col("g.median_hours_since_open"), 2).alias("hours_since_open_minus_median"),
    )
)

summary = (
    above_average
    .groupBy("priority")
    .agg(
        F.round(F.sum("hours_since_open_minus_median"), 2).alias("total_hours_since_open_minus_median"),
        F.sum(F.when(F.col("event_type") != "created", F.col("median_hours_since_open"))).alias("median_hours_since_open_event_type_created"),
        F.sum("hours_since_open_minus_median").alias("total_hours_since_open_minus_median_2"),
        F.sum(F.when(F.col("event_type") == "resolved", 1).otherwise(0)).alias("n_event_type_resolved"),
        F.round(F.sum("hours_since_open"), 2).alias("total_hours_since_open"),
        F.sum("median_hours_since_open").alias("sum_median_hours_since_open_check"),
    )
    .filter(F.col("sum_median_hours_since_open_check") > 28.8)
    .drop("sum_median_hours_since_open_check")
)

result = (
    summary
    .filter(F.col("total_hours_since_open_minus_median_2") <= 306.0)
    .select(
        "priority",
        "total_hours_since_open_minus_median",
        "median_hours_since_open_event_type_created",
        "n_event_type_resolved",
        F.round(F.col("n_event_type_resolved") / F.nullif(F.col("total_hours_since_open"), F.lit(0)), 3).alias("n_event_type_resolved_per_total_hours_since_open"),
    )
    .orderBy(
        "median_hours_since_open_event_type_created",
        "n_event_type_resolved_per_total_hours_since_open",
    )
)
