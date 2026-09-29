from pyspark.sql import functions as F

valid_holders = (
    df_policyholders
    .filter(F.col("risk_score") >= 0.3)
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
        F.when(F.col("segment").isin("RETAIL", "VIP"), "group_a").when(F.col("segment") == "SME", "group_b").otherwise("other").alias("segment_group"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(F.count("*").alias("policy_count"))
)

with_counts = (
    valid_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
    )
)

policy_stats_2 = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count_2"),
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("policy_status_count"),
        F.count("*").alias("policy_count_3"),
    )
)

with_stats = (
    with_counts.alias("s")
    .join(policy_stats_2.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
        F.coalesce(F.col("p.policy_status_count"), F.lit(0)).alias("policy_status_count"),
        F.coalesce(F.col("p.policy_count_3"), F.lit(0)).alias("policy_count_3"),
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.concat_ws("-", F.col("region"), F.col("segment")).alias("region_segment_label"),
        F.round(F.col("policy_count_2") / F.nullif(F.col("policy_count_3"), F.lit(0)), 3).alias("policy_count_2_per_policy_count_3"),
        F.date_format(F.col("joined_on"), "MMM yyyy").alias("joined_on_label"),
    )
)

result = (
    scored
    .groupBy("segment_group", "policy_count_2")
    .pivot("segment", ["VIP", "SME", "RETAIL"])
    .agg(F.count("policy_count"))
)
