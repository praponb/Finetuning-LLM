from pyspark.sql import functions as F

recent_agents = (
    df_support_agents
    .filter((F.lower(F.col("agent_name")).like("%t %")) & (F.col("tier") == "T1"))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
        F.date_format(F.col("hired_on"), "yyyy-MM-dd").alias("hired_on_label"),
    )
)

totals = (
    recent_agents
    .groupBy("tier", "team_or_unknown")
    .agg(
        F.round(100 * F.count(F.when(F.col("tier") == "T2", 1)) / F.count("*"), 1).alias("pct_tier_t2"),
        F.countDistinct("tier").alias("distinct_tiers"),
        F.array_sort(F.collect_set("team")).alias("team_list"),
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("tier")).alias("tier_list"),
    )
)

derived = (
    totals
    .filter(F.size(F.col("tier_list")) >= 1)
    .select(
        "*",
        F.when(F.col("pct_tier_t2") >= 85.0, "HIGH").when(F.col("pct_tier_t2") >= 25.0, "MEDIUM").otherwise("LOW").alias("pct_tier_t2_tier"),
        (F.floor(F.col("n_rows") / 2) * 2).alias("n_rows_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("team_or_unknown")), " ", "_")).alias("team_or_unknown_slug"),
    )
)

exploded = (
    derived
    .select(
        "tier",
        "team_or_unknown",
        "pct_tier_t2",
        "distinct_tiers",
        "team_list",
        "n_rows",
        "pct_tier_t2_tier",
        "n_rows_bucket",
        "team_or_unknown_slug",
        F.posexplode("tier_list").alias("tier_list_item_pos", "tier_list_item"),
    )
)

result = (
    exploded
    .filter(F.col("tier_list_item_pos") > 1)
    .select(
        "tier",
        "team_or_unknown",
        "pct_tier_t2",
        "distinct_tiers",
        "team_list",
        "n_rows",
        "tier_list_item_pos",
        "tier_list_item",
    )
    .orderBy("pct_tier_t2")
)
