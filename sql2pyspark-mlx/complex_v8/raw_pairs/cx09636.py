from pyspark.sql import functions as F

valid_tickets = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "channel",
        "tags",
        F.round(F.col("requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("requester_id_per_csat"),
        F.date_format(F.col("closed_at"), "yyyy-MM-dd").alias("closed_at_label"),
    )
)

rollup_base = (
    valid_tickets
    .groupBy("channel", "agent_id")
    .agg(
        F.sum(F.when(F.col("channel").isNotNull(), F.col("requester_id_per_csat"))).alias("requester_id_per_csat_channel_is_not_null"),
        F.round(F.sum("requester_id_per_csat") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("requester_id_per_csat_to_requester_id_ratio"),
        F.round(F.sum("requester_id") / F.nullif(F.sum("requester_id_per_csat"), F.lit(0)), 3).alias("requester_id_to_requester_id_per_csat_ratio"),
        F.countDistinct("agent_id").alias("distinct_agent_ids"),
        F.sum("requester_id_per_csat").alias("total_requester_id_per_csat"),
        F.count("*").alias("cnt"),
    )
    .filter(F.col("cnt") >= 2)
)

enriched = (
    rollup_base
    .select(
        "*",
        F.substring(F.trim(F.col("channel")), 1, 3).alias("channel_prefix"),
        F.when(F.col("distinct_agent_ids") >= 8, "large").when(F.col("distinct_agent_ids") >= 3, "medium").otherwise("small").alias("distinct_agent_ids_tier"),
        F.coalesce(F.col("cnt"), F.lit(0)).alias("cnt_filled"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("opened_at").alias("first_ticket_opened_at"),
        F.sum("csat").alias("total_ticket_csat"),
    )
)

profiled = (
    enriched.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_opened_at", "t.total_ticket_csat")
)

with_parent_agent = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.channel",
        "s.agent_id",
        "s.requester_id_per_csat_to_requester_id_ratio",
        "s.distinct_agent_ids",
        "s.total_requester_id_per_csat",
        "s.channel_prefix",
        "s.distinct_agent_ids_tier",
        "s.cnt_filled",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

distinct_agent_ids_tier_stats = (
    with_parent_agent
    .groupBy("distinct_agent_ids_tier")
    .agg(F.avg("distinct_agent_ids").alias("avg_distinct_agent_ids"))
)

vs_group = (
    with_parent_agent.alias("s")
    .join(distinct_agent_ids_tier_stats.alias("g"), F.col("g.distinct_agent_ids_tier") == F.col("s.distinct_agent_ids_tier"), "inner")
    .filter(F.col("s.distinct_agent_ids") > F.col("g.avg_distinct_agent_ids"))
    .select(
        "s.*",
        "g.avg_distinct_agent_ids",
        F.round(F.col("s.distinct_agent_ids") - F.col("g.avg_distinct_agent_ids"), 2).alias("distinct_agent_ids_minus_avg"),
    )
)

tagged = (
    vs_group
    .select(
        "*",
        F.substring(F.trim(F.col("agent_tier")), 1, 4).alias("agent_tier_prefix"),
        (F.floor(F.col("distinct_agent_ids") / 2) * 2).alias("distinct_agent_ids_bucket"),
        F.coalesce(F.col("avg_distinct_agent_ids"), F.lit(0.0)).alias("avg_distinct_agent_ids_filled"),
    )
)

result = (
    tagged
    .select(
        "channel",
        "agent_id",
        "requester_id_per_csat_to_requester_id_ratio",
        "distinct_agent_ids",
        "total_requester_id_per_csat",
        "channel_prefix",
        "distinct_agent_ids_tier",
        "cnt_filled",
        "agent_team",
        "avg_distinct_agent_ids",
        "distinct_agent_ids_minus_avg",
        "agent_tier_prefix",
        "distinct_agent_ids_bucket",
        "avg_distinct_agent_ids_filled",
    )
    .orderBy(
        "distinct_agent_ids",
        F.col("distinct_agent_ids_bucket").asc_nulls_last(),
    )
)
