from pyspark.sql import functions as F

cleaned = (
    df_tickets
    .filter((F.month(F.col("closed_at")).isin(2, 3)) & (F.col("csat") < 5))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        F.lower(F.regexp_replace(F.trim(F.col("channel")), " ", "_")).alias("channel_slug"),
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("closed_at")).alias("days_since_closed_at"),
    )
)

detail_rows_events = (
    cleaned.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel_slug",
        "s.days_since_closed_at",
        F.col("t.id").alias("event_id"),
        "t.event_at",
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_stats = (
    detail_rows_events.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

agent_id_stats = (
    with_stats
    .groupBy("agent_id")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

compared = (
    with_stats.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

totals = (
    compared
    .groupBy(
        F.date_format("event_at", "yyyy-MM").alias("event_at_ym"),
        "event_id",
        "ticket_count",
    )
    .agg(
        F.round(F.stddev("requester_id"), 2).alias("stddev_requester_id"),
        F.round(F.avg("avg_requester_id"), 2).alias("avg_avg_requester_id"),
        F.count("*").alias("row_count"),
        F.sum("requester_id_minus_avg").alias("total_requester_id_minus_avg"),
    )
)

result = (
    totals
    .filter(F.col("total_requester_id_minus_avg") <= 88.8)
    .select(
        "event_at_ym",
        "event_id",
        "ticket_count",
        "avg_avg_requester_id",
        "row_count",
        "total_requester_id_minus_avg",
        F.when(F.col("avg_avg_requester_id") >= 17.8, "large").when(F.col("avg_avg_requester_id") >= 9.4, "medium").otherwise("small").alias("avg_avg_requester_id_tier"),
        F.length(F.trim(F.col("event_at_ym"))).alias("event_at_ym_len"),
    )
)
