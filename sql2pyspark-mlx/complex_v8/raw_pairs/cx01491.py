from pyspark.sql import functions as F

valid_policys = (
    df_policies
    .filter(F.year(F.col("end_date")) == 2026)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status",
    )
)

policy_stats = (
    df_policies
    .groupBy("holder_id")
    .agg(
        F.round(F.avg("premium"), 2).alias("avg_policy_premium"),
        F.count("*").alias("policy_count"),
        F.count("*").alias("policy_count_2"),
    )
)

with_counts = (
    valid_policys.alias("s")
    .join(policy_stats.alias("p"), F.col("p.holder_id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        "p.avg_policy_premium",
        F.coalesce(F.col("p.policy_count"), F.lit(0)).alias("policy_count"),
        F.coalesce(F.col("p.policy_count_2"), F.lit(0)).alias("policy_count_2"),
    )
)

tagged = (
    with_counts
    .select(
        "id",
        "holder_id",
        "product",
        "policy_count",
        "policy_count_2",
        F.lower(F.trim(F.col("status"))).alias("status_clean"),
        F.coalesce(F.col("avg_policy_premium"), F.lit(0.0)).alias("avg_policy_premium_filled"),
        F.when(F.col("premium") >= 3817.5, "large").when(F.col("premium") >= 2033.8, "medium").otherwise("small").alias("premium_tier"),
    )
)

filtered = (
    tagged.alias("s")
    .join(df_claims.alias("c"), (F.col("c.policy_id") == F.col("s.id")) & (F.col("c.claim_date").between(F.lit("2024-06-07").cast("date"), F.lit("2025-11-15").cast("date"))), "left_anti")
)

result = (
    filtered
    .filter(F.col("policy_count_2").between(2, 6))
    .select(
        "id",
        "holder_id",
        "product",
        "policy_count",
        "policy_count_2",
        "premium_tier",
        F.initcap(F.trim(F.col("premium_tier"))).alias("premium_tier_clean"),
    )
    .orderBy(F.col("id").asc_nulls_last(), F.col("policy_count").asc_nulls_last())
)
