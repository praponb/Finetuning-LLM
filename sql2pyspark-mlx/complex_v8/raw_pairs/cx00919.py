from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.col("claimed_amount") < 39666.5) & (F.col("status").isin("OPEN", "IN_REVIEW")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.greatest(F.col("claimed_amount"), F.col("approved_amount")).alias("greatest_claimed_amount_approved_amount"),
    )
)

qualified = (
    filtered_claims.alias("s")
    .join(df_claims.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left_semi")
)

result = (
    qualified
    .groupingSets(
        [["status", "adjuster_id"], ["status"], ["adjuster_id"], []],
        "status", "adjuster_id",
    )
    .agg(
        F.sum("status_len").alias("total_status_len"),
        F.min("greatest_claimed_amount_approved_amount").alias("min_greatest_claimed_amount_approved_amount"),
        F.round(F.sum("days_since_reported_at") / F.nullif(F.sum("greatest_claimed_amount_approved_amount"), F.lit(0)), 3).alias("days_since_reported_at_to_greatest_claimed_amoun"),
    )
    .orderBy(F.col("status").asc_nulls_last(), F.col("adjuster_id").asc_nulls_last())
)
