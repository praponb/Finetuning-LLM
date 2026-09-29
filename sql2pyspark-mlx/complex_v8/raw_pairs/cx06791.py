from pyspark.sql import functions as F

filtered = (
    df_tickets
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("csat").cast("string")).alias("channel_ref"),
        (F.col("csat") + F.col("requester_id")).alias("csat_plus_requester_id"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_at")).alias("days_since_opened_at"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.channel",
        "s.csat_plus_requester_id",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.count("*").alias("ticket_count"))
)

with_counts = (
    joined.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_count"), F.lit(0)).alias("ticket_count"),
    )
)

grouped = (
    with_counts
    .groupBy(F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(F.col("priority").isNotNull(), F.col("csat_plus_requester_id"))).alias("csat_plus_requester_id_priority_is_not_null"),
        F.max("csat_plus_requester_id").alias("max_csat_plus_requester_id"),
        F.percentile_approx("csat_plus_requester_id", 0.5).alias("median_csat_plus_requester_id"),
    )
)

result = (
    grouped
    .select(
        "agent_hired_on_ym",
        "csat_plus_requester_id_priority_is_not_null",
        "distinct_ids",
    )
    .unpivot(
        ids=["agent_hired_on_ym"],
        values=["csat_plus_requester_id_priority_is_not_null", "distinct_ids"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
