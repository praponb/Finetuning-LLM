from pyspark.sql import functions as F

scoped = (
    df_support_agents
    .filter(F.col("tier") == "T1")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_format(F.col("hired_on"), "yyyy-MM").alias("hired_on_label"),
        F.substring(F.trim(F.col("tier")), 1, 4).alias("tier_prefix"),
        F.when(F.col("tier").isin("T2", "T3"), "group_a").when(F.col("tier") == "T1", "group_b").otherwise("other").alias("tier_group"),
    )
)

scored = (
    scoped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("tier_group")), " ", "_")).alias("tier_group_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("tier_group")), " ", "_")).alias("tier_group_slug_2"),
        F.concat_ws("|", F.col("agent_name"), F.col("team")).alias("agent_name_team_label"),
    )
)

labeled = (
    scored
    .select(
        "*",
        F.concat_ws("|", F.col("tier_group"), F.col("tier")).alias("tier_group_tier_label"),
        F.lower(F.regexp_replace(F.trim(F.col("agent_name")), " ", "_")).alias("agent_name_slug"),
    )
)

result = (
    labeled
    .rollup("tier", "team")
    .agg(
        F.grouping("tier").alias("is_total_tier"),
        F.array_sort(F.collect_set("agent_name")).alias("agent_name_list"),
        F.sum(F.when(F.col("team").isNotNull(), 1).otherwise(0)).alias("n_team_is_not_null"),
        F.count(F.when(F.col("tier") != "T1", 1)).alias("n_tier_t1"),
    )
)
