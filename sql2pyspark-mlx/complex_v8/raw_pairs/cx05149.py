from pyspark.sql import functions as F

recent_holders = (
    df_policyholders
    .filter((F.lower(F.col("holder_name")).like("%ak%")) & (F.col("region") == "South"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.add_months(F.col("joined_on"), 6).alias("joined_on_plus_6m"),
        F.date_add(F.col("joined_on"), 7).alias("joined_on_plus_7d"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.max("end_date").alias("last_policy_end_date"),
        F.count(F.when(F.col("product") == "TRAVEL", 1)).alias("policy_product_count"),
        F.min("start_date").alias("first_policy_start_date"),
    )
)

with_counts = (
    recent_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_policy_end_date",
        F.coalesce(F.col("p.policy_product_count"), F.lit(0)).alias("policy_product_count"),
        "p.first_policy_start_date",
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(F.max("premium").alias("max_policy_premium"))
)

profiled = (
    with_counts.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select("s.*", "p.max_policy_premium")
)

result = (
    profiled
    .groupBy("segment", "first_policy_start_date")
    .pivot("region", ["Central", "West", "North", "South"])
    .agg(F.max("policy_product_count"))
    .orderBy("segment", "first_policy_start_date")
)
