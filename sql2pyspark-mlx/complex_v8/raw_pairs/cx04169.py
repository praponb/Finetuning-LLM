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

tagged = (
    matched
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "*",
        F.when(F.col("hours_since_open") >= 57.6, "A").when(F.col("hours_since_open") >= 25.2, "B").otherwise("C").alias("hours_since_open_tier"),
    )
)

per_group = (
    tagged
    .groupBy(
        F.date_trunc("month", "event_at").alias("event_at_month"),
        "hours_since_open_tier",
        "ticket_id",
    )
    .agg(
        F.percentile_approx("hours_since_open", 0.5).alias("median_hours_since_open"),
        F.count("*").alias("n_rows"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    per_group
    .select(
        "event_at_month",
        "hours_since_open_tier",
        "ticket_id",
        "median_hours_since_open",
        "n_rows",
        F.round(F.col("n_rows") / F.nullif(F.col("median_hours_since_open"), F.lit(0)), 3).alias("n_rows_per_median_hours_since_open"),
    )
    .orderBy(
        F.col("ticket_id").asc(),
        "median_hours_since_open",
        "event_at_month",
        "hours_since_open_tier",
    )
    .limit(20)
)
