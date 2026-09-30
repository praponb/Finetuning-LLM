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

scored = (
    joined_ranges
    .select(
        "*",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.when(F.col("hours_since_open") >= 39.6, "large").when(F.col("hours_since_open") >= 18.0, "medium").otherwise("small").alias("hours_since_open_tier"),
    )
)

priority_stats = (
    scored
    .groupBy("priority")
    .agg(F.avg("days_since_event_at").alias("avg_days_since_event_at"))
)

vs_group = (
    scored.alias("s")
    .join(priority_stats.alias("g"), F.col("g.priority") == F.col("s.priority"), "inner")
    .filter(F.col("s.days_since_event_at") > F.col("g.avg_days_since_event_at"))
    .select(
        "s.*",
        "g.avg_days_since_event_at",
        F.round(F.col("s.days_since_event_at") - F.col("g.avg_days_since_event_at"), 2).alias("days_since_event_at_minus_avg"),
    )
)

result = (
    vs_group
    .select(
        "id",
        F.col("avg_days_since_event_at").alias("m_avg_days_since_event_at"),
        F.col("hours_since_open").alias("hours_since_open_v"),
        F.col("days_since_event_at_minus_avg").alias("days_since_event_at_minus_avg_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_avg_days_since_event_at", "hours_since_open_v", "days_since_event_at_minus_avg_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
