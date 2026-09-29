from pyspark.sql import functions as F

valid = (
    df_ticket_events
    .filter((F.col("event_type") == "escalated") & (F.col("event_at") >= F.lit("2025-02-06 09:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("event_at")).alias("days_since_event_at"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("event_at")).alias("days_since_event_at_2"),
    )
)

enriched_ticket = (
    valid.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.actor",
        "s.days_since_event_at_2",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

rollup_base = (
    enriched_ticket
    .filter(F.lower(F.col("agent_name")).like("%ir%"))
    .groupBy("agent_tier")
    .agg(
        F.percentile_approx("days_since_event_at_2", 0.5).alias("median_days_since_event_at_2"),
        F.max("days_since_event_at_2").alias("max_days_since_event_at_2"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
    )
)

qualified = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

tagged = (
    qualified
    .filter(F.col("cnt") <= 8)
    .select(
        "*",
        F.concat(F.col("agent_tier"), F.lit("#"), F.col("cnt").cast("string")).alias("agent_tier_ref"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
    )
)

derived = (
    tagged
    .select(
        "*",
        F.coalesce(F.col("agent_tier"), F.lit("UNKNOWN")).alias("agent_tier_or_unknown"),
        (F.floor(F.col("cnt") / 2) * 2).alias("cnt_bucket"),
    )
)

agent_tier_or_unknown_stats = (
    derived
    .groupBy("agent_tier_or_unknown")
    .agg(F.avg("median_days_since_event_at_2").alias("avg_median_days_since_event_at_2"))
)

above_average = (
    derived.alias("s")
    .join(agent_tier_or_unknown_stats.alias("g"), F.col("g.agent_tier_or_unknown") == F.col("s.agent_tier_or_unknown"), "inner")
    .filter(F.col("s.median_days_since_event_at_2") > F.col("g.avg_median_days_since_event_at_2"))
    .select(
        "s.*",
        "g.avg_median_days_since_event_at_2",
        F.round(F.col("s.median_days_since_event_at_2") - F.col("g.avg_median_days_since_event_at_2"), 2).alias("median_days_since_event_at_2_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "agent_tier",
        "median_days_since_event_at_2",
        "max_days_since_event_at_2",
        "agent_name_list",
        "agent_tier_slug",
        "agent_tier_or_unknown",
        "median_days_since_event_at_2_minus_avg",
    )
    .orderBy(
        F.col("agent_tier_slug").desc(),
        F.col("median_days_since_event_at_2").asc_nulls_last(),
    )
)
