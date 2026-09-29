from pyspark.sql import functions as F

prepared = (
    df_tickets
    .filter(F.col("requester_id").between(2, 25))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "csat",
        "tags",
    )
)

without_matches = (
    prepared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left_semi")
)

agent_id_stats = (
    without_matches
    .groupBy("agent_id")
    .agg(F.avg("csat").alias("avg_csat"))
)

outliers = (
    without_matches.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") > F.col("g.avg_csat"))
    .select(
        "s.*",
        "g.avg_csat",
        F.round(F.col("s.csat") - F.col("g.avg_csat"), 2).alias("csat_minus_avg"),
    )
)

with_parent = (
    outliers.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.upper(F.trim(F.col("su.agent_name"))).like("L%")), "inner")
    .filter(F.col("su.hired_on") < F.lit("2024-12-31").cast("date"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.csat_minus_avg",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

with_activity = (
    with_parent.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-05-02").cast("date"), F.lit("2025-02-27").cast("date"))), "left_anti")
)

with_details_agent = (
    with_activity.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.csat",
        "s.agent_tier",
        "s.agent_hired_on",
        F.col("su.tier").alias("agent_tier_2"),
    )
)

agent_id_stats_2 = (
    with_details_agent
    .groupBy("agent_id")
    .agg(F.percentile_approx("csat", 0.5).alias("median_csat"))
)

compared = (
    with_details_agent.alias("s")
    .join(agent_id_stats_2.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.csat") >= F.col("g.median_csat"))
    .select(
        "s.*",
        "g.median_csat",
        F.round(F.col("s.csat") - F.col("g.median_csat"), 2).alias("csat_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "csat",
        "agent_tier",
        "median_csat",
        F.abs(F.col("csat_minus_median") - -2.0).alias("csat_minus_median_distance"),
        F.trunc(F.col("agent_hired_on"), "MM").alias("agent_hired_on_month_start"),
    )
)
