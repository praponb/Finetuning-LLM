from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("claim_type").isin("fire", "collision", "medical"), 1)).alias("claim_claim_type_count"),
        F.count("*").alias("claim_count"),
        F.sum("approved_amount").alias("total_claim_approved_amount"),
    )
)

with_counts = (
    scoped.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        "c.total_claim_approved_amount",
    )
)

tagged = (
    with_counts
    .select(
        "id",
        "product",
        "premium",
        "start_date",
        "claim_claim_type_count",
        "claim_count",
        "total_claim_approved_amount",
        (F.col("claim_count") + F.col("premium")).alias("claim_count_plus_premium"),
        F.round(F.col("premium") * 1.07, 2).alias("premium_adj"),
    )
)

derived = (
    tagged
    .select(
        "id",
        "premium",
        "start_date",
        "claim_claim_type_count",
        "total_claim_approved_amount",
        "claim_count_plus_premium",
        "premium_adj",
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.when(F.col("claim_count_plus_premium") >= 3004.8, "large").when(F.col("claim_count_plus_premium") >= 1252.0, "medium").otherwise("small").alias("claim_count_plus_premium_tier"),
    )
)

result = (
    derived
    .select(
        "id",
        "premium",
        "start_date",
        "claim_claim_type_count",
        "claim_count_plus_premium",
        "premium_adj",
        "premium_filled",
        "claim_count_plus_premium_tier",
        F.least(F.col("premium"), F.col("premium_filled")).alias("least_premium_premium_filled"),
        F.dayofweek(F.col("start_date")).alias("start_date_dayofweek"),
    )
    .orderBy(F.col("start_date_dayofweek").desc())
)
