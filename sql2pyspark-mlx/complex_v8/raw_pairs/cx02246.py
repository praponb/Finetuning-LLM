from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("status").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
    )
)

claim_type_stats = (
    valid_claims
    .groupBy("claim_type")
    .agg(F.avg("approved_amount").alias("avg_approved_amount"))
)

outliers = (
    valid_claims.alias("s")
    .join(claim_type_stats.alias("g"), F.col("g.claim_type") == F.col("s.claim_type"), "inner")
    .filter(F.col("s.approved_amount") > F.col("g.avg_approved_amount"))
    .select(
        "s.*",
        "g.avg_approved_amount",
        F.round(F.col("s.approved_amount") - F.col("g.avg_approved_amount"), 2).alias("approved_amount_minus_avg"),
    )
)

summary = (
    outliers
    .groupBy("policy_id", "adjuster_id", "claim_type")
    .agg(
        F.round(F.sum("avg_approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("avg_approved_amount_to_claimed_amount_ratio"),
        F.sum("approved_amount_minus_avg").alias("total_approved_amount_minus_avg"),
        F.sum(F.when(F.col("status") != "OPEN", F.col("avg_approved_amount")).otherwise(0)).alias("avg_approved_amount_status_open"),
        F.max("approved_amount").alias("max_approved_amount_check"),
    )
    .filter(F.col("max_approved_amount_check") > 4234.9)
    .drop("max_approved_amount_check")
)

result = (
    summary
    .groupingSets(
        [["adjuster_id", "claim_type"], ["adjuster_id"], ["claim_type"], []],
        "adjuster_id", "claim_type",
    )
    .agg(
        F.sum(F.when(F.col("claim_type").isin("collision", "theft", "flood"), 1).otherwise(0)).alias("n_claim_type_in_collision_thef"),
    )
)
