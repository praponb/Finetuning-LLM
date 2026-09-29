from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.coalesce(F.col("approved_amount"), F.lit(0.0)).alias("approved_amount_filled"),
        (F.col("approved_amount") - F.col("claimed_amount")).alias("approved_amount_minus_claimed_amount"),
        F.concat_ws(": ", F.col("claim_type"), F.col("status")).alias("claim_type_status_label"),
    )
)

rollup_base = (
    scoped_claims
    .groupBy(
        "status",
        "adjuster_id",
        F.date_format("claim_date", "yyyy-MM").alias("claim_date_ym"),
    )
    .agg(
        F.round(F.sum("approved_amount_minus_claimed_amount") / F.nullif(F.sum("approved_amount_filled"), F.lit(0)), 3).alias("approved_amount_minus_claimed_amount_to_approved"),
        F.max("claimed_amount").alias("max_claimed_amount"),
        F.max("claim_date").alias("last_claim_date"),
        F.max("approved_amount_minus_claimed_amount").alias("max_approved_amount_minus_claimed_amount"),
        F.max("approved_amount").alias("max_approved_amount"),
        F.avg("approved_amount_filled").alias("avg_approved_amount_filled_check"),
    )
    .filter(F.col("avg_approved_amount_filled_check") > 5000.0)
    .drop("avg_approved_amount_filled_check")
)

qualified = (
    rollup_base.alias("s")
    .join(df_claims.alias("c"), (F.col("c.adjuster_id") == F.col("s.adjuster_id")) & (F.col("c.reported_at") >= F.lit("2025-01-19 14:00:00").cast("timestamp")), "left_anti")
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count(F.when(F.col("claim_type").isNotNull(), 1)).alias("claim_claim_type_count"),
    )
)

profiled = (
    qualified.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

result = (
    profiled
    .select(
        "status",
        "claim_date_ym",
        "approved_amount_minus_claimed_amount_to_approved",
        "last_claim_date",
        "max_approved_amount_minus_claimed_amount",
        (F.col("max_claimed_amount") - F.col("claim_claim_type_count")).alias("max_claimed_amount_minus_claim_claim_type_count"),
    )
    .orderBy(
        "status",
        F.col("approved_amount_minus_claimed_amount_to_approved").asc(),
    )
)
