from pyspark.sql import functions as F

valid_agents = (
    df_support_agents
    .filter((F.col("team") == "VIP") & (F.upper(F.trim(F.col("agent_name"))).like("G%")))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.date_trunc("month", F.col("hired_on")).alias("hired_on_month"),
    )
)

derived = (
    valid_agents
    .filter(F.lower(F.col("agent_name")).like("%an%"))
    .select(
        "*",
        F.date_trunc("quarter", F.col("hired_on_month")).alias("hired_on_month_quarter"),
        F.concat_ws(" / ", F.col("agent_name"), F.col("tier")).alias("agent_name_tier_label"),
        F.concat(F.col("tier"), F.lit("#"), F.col("id").cast("string")).alias("tier_ref"),
    )
)

summary = (
    derived
    .groupBy(
        F.date_trunc("month", "hired_on_month_quarter").alias("hired_on_month_quarter_month"),
        "tier",
    )
    .agg(
        F.countDistinct("tier").alias("distinct_tiers"),
        F.array_sort(F.collect_set("tier")).alias("tier_list"),
        F.sum(F.when(F.col("tier") == "T1", 1).otherwise(0)).alias("n_tier_t1"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("tier") != "T3", 1)).alias("n_tier_t3"),
    )
    .filter(F.col("row_count") >= 2)
)

per_label = (
    summary
    .select(
        "hired_on_month_quarter_month",
        "tier",
        "distinct_tiers",
        "n_tier_t1",
        "row_count",
        "n_tier_t3",
        F.posexplode("tier_list").alias("tier_list_item_pos", "tier_list_item"),
    )
)

result = (
    per_label
    .select(
        "hired_on_month_quarter_month",
        "distinct_tiers",
        "tier_list_item_pos",
        "tier_list_item",
        F.upper(F.trim(F.col("tier_list_item"))).alias("tier_list_item_clean"),
        F.least(F.col("distinct_tiers"), F.col("n_tier_t1")).alias("least_distinct_tiers_n_tier_t1"),
    )
    .orderBy(F.col("hired_on_month_quarter_month").desc())
)
