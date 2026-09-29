from pyspark.sql import functions as F

matched = (
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

qualified = (
    matched.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp")), "left_anti")
)

tagged = (
    qualified
    .select(
        "*",
        F.concat_ws(" / ", F.col("event_type"), F.col("priority")).alias("event_type_priority_label"),
        F.when(F.col("hours_since_open") >= 50.4, "HIGH").when(F.col("hours_since_open") >= 14.4, "MEDIUM").otherwise("LOW").alias("hours_since_open_tier"),
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

event_type_stats = (
    tagged
    .groupBy("event_type")
    .agg(F.percentile_approx("event_type_len", 0.5).alias("median_event_type_len"))
)

vs_group = (
    tagged.alias("s")
    .join(event_type_stats.alias("g"), F.col("g.event_type") == F.col("s.event_type"), "inner")
    .filter(F.col("s.event_type_len") >= F.col("g.median_event_type_len"))
    .select(
        "s.*",
        "g.median_event_type_len",
        F.round(F.col("s.event_type_len") - F.col("g.median_event_type_len"), 2).alias("event_type_len_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "ticket_id",
        "event_at",
        "priority",
        "agent_id",
        "hours_since_open",
        "event_type_priority_label",
        "hours_since_open_tier",
        "median_event_type_len",
    )
    .orderBy(F.col("median_event_type_len").asc_nulls_last(), "hours_since_open")
)
