from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 4) | (F.year(F.col("claim_date")) == 2024))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "reported_at",
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

derived = (
    cleaned_claims
    .select(
        "*",
        F.when(F.col("claimed_amount_bucket") >= 35030, "A").when(F.col("claimed_amount_bucket") >= 20060, "B").otherwise("C").alias("claimed_amount_bucket_tier"),
        F.when(F.col("claim_type").isin("collision", "medical"), "group_a").when(F.col("claim_type") == "fire", "group_b").otherwise("other").alias("claim_type_group"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

result = (
    derived
    .groupingSets(
        [["claim_type_group", "claim_type", "claimed_amount_bucket_tier"], ["claim_type_group"], ["claim_type"]],
        "claim_type_group", "claim_type", "claimed_amount_bucket_tier",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("claimed_amount") / F.nullif(F.sum("days_since_reported_at"), F.lit(0)), 3).alias("claimed_amount_to_days_since_reported_at_ratio"),
    )
    .select(
        F.coalesce(F.col("claim_type_group"), F.lit("ALL")).alias("claim_type_group"),
        F.coalesce(F.col("claim_type"), F.lit("ALL")).alias("claim_type"),
        F.coalesce(F.col("claimed_amount_bucket_tier"), F.lit("ALL")).alias("claimed_amount_bucket_tier"),
        "grouping_level",
        "claimed_amount_to_days_since_reported_at_ratio",
    )
)
