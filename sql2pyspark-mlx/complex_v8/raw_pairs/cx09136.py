from pyspark.sql import functions as F

prepared_tickets = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "upgrade")) & (F.col("requester_id").between(2, 21)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        F.date_trunc("quarter", F.col("opened_at")).alias("opened_at_quarter"),
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
    )
)

filtered = (
    prepared_tickets.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left_semi")
)

with_parent_agent = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T2", "T3"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.opened_at_quarter",
        "s.csat_filled",
        F.col("su.tier").alias("agent_tier"),
    )
)

agent_tier_stats = (
    with_parent_agent
    .groupBy("agent_tier")
    .agg(F.max("csat_filled").alias("max_csat_filled"))
)

above_average = (
    with_parent_agent.alias("s")
    .join(agent_tier_stats.alias("g"), F.col("g.agent_tier") == F.col("s.agent_tier"), "inner")
    .filter(F.col("s.csat_filled") == F.col("g.max_csat_filled"))
    .select(
        "s.*",
        "g.max_csat_filled",
        F.round(F.col("s.csat_filled") - F.col("g.max_csat_filled"), 2).alias("csat_filled_minus_max"),
    )
)

combined_details_agent = (
    above_average.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        "su.agent_name",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
    )
)

totals = (
    combined_details_agent
    .groupBy("csat_filled", "priority")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("agent_team")).alias("agent_team_list"),
        F.round(F.sum("csat_filled") / F.nullif(F.sum("requester_id"), F.lit(0)), 3).alias("csat_filled_to_requester_id_ratio"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
    )
)

result = (
    totals
    .select(
        "csat_filled",
        "priority",
        "row_count",
        "distinct_agent_tiers",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.array_join(F.array_sort(F.col("agent_team_list")), ",").alias("agent_team_list_text"),
    )
    .orderBy(F.col("csat_filled").asc_nulls_last(), "priority")
    .limit(5)
)
