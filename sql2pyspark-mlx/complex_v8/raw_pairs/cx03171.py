from pyspark.sql import functions as F

recent = (
    df_claims
    .filter(F.col("claim_type") != "flood")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.abs(F.col("claimed_amount") - 17585.2).alias("claimed_amount_distance"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
        F.add_months(F.col("claim_date"), 3).alias("claim_date_plus_3m"),
    )
)

enriched = (
    recent
    .filter(F.col("claimed_amount_distance") < 27500.0)
    .select(
        "*",
        F.when(F.col("claimed_amount_distance") >= 27500.0, "HIGH").when(F.col("claimed_amount_distance") >= 22500.0, "MEDIUM").otherwise("LOW").alias("claimed_amount_distance_tier"),
        F.lower(F.trim(F.col("status_ref"))).alias("status_ref_clean"),
    )
)

result = (
    enriched
    .cube("policy_id", "adjuster_id", "claim_type")
    .agg(
        F.max("claimed_amount_distance").alias("max_claimed_amount_distance"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount_distance"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_distance_ratio"),
        F.sum(F.when(F.col("status").isin("APPROVED", "REJECTED"), 1).otherwise(0)).alias("n_status_in_approved_rejected"),
    )
    .orderBy(
        F.col("policy_id").asc_nulls_last(),
        F.col("adjuster_id").asc_nulls_last(),
        F.col("claim_type").asc_nulls_last(),
    )
)
