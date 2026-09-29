from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter((F.col("risk_score").between(0.2, 0.6)) & (F.col("holder_name").isNotNull()))
    .select(
        "id",
        "risk_score",
        "joined_on",
        "segment",
        F.coalesce(F.col("risk_score"), F.lit(0.0)).alias("risk_score_filled"),
        F.date_format(F.col("joined_on"), "yyyy-MM").alias("joined_on_label"),
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.count("*").alias("policy_count"),
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
    )
)

with_counts = (
    cleaned_holders.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        "p.avg_policy_premium",
    )
)

qualified = (
    with_counts.alias("s")
    .join(df_policies.alias("p"), (F.col("p.holder_id") == F.col("s.id")) & (F.col("p.product") != "LIFE") & (F.col("p.status").isNotNull()), "left_semi")
)

result = (
    qualified
    .groupBy("joined_on")
    .pivot("segment", ["RETAIL", "VIP"])
    .agg(F.min("avg_policy_premium"))
    .orderBy("joined_on")
)
