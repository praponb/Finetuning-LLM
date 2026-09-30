from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.col("claim_date") < F.lit("2025-04-11").cast("date")) & (F.col("approved_amount").between(0.0, 33690.0)))
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
    )
)

rollup_base = (
    scoped
    .groupBy(F.date_trunc("month", "reported_at").alias("reported_at_month"))
    .agg(
        F.count(F.when(F.col("claim_type") == "flood", 1)).alias("n_claim_type_flood"),
        F.round(F.sum("approved_amount") / F.nullif(F.sum("claimed_amount"), F.lit(0)), 3).alias("approved_amount_to_claimed_amount_ratio"),
        F.sum(F.when(F.col("status") != "PAID", 1).otherwise(0)).alias("n_status_paid"),
        F.sum(F.when(F.col("claim_type") == "medical", F.col("claimed_amount"))).alias("claimed_amount_claim_type_medical"),
        F.round(F.sum("approved_amount"), 2).alias("total_approved_amount"),
        F.max("approved_amount").alias("max_approved_amount_check"),
    )
    .filter(F.col("max_approved_amount_check") > 0.0)
    .drop("max_approved_amount_check")
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("approved_amount_to_claimed_amount_ratio").alias("overall_avg_approved_amount_to_claimed_amount_ra")))
    .filter(F.col("approved_amount_to_claimed_amount_ratio") > F.col("overall_avg_approved_amount_to_claimed_amount_ra"))
    .drop("overall_avg_approved_amount_to_claimed_amount_ra")
)

result = (
    with_activity
    .select(
        "reported_at_month",
        "n_claim_type_flood",
        "n_status_paid",
        "claimed_amount_claim_type_medical",
        "total_approved_amount",
        (F.floor(F.col("total_approved_amount") / 50000) * 50000).alias("total_approved_amount_bucket"),
        (F.col("total_approved_amount") + F.col("n_claim_type_flood")).alias("total_approved_amount_plus_n_claim_type_flood"),
    )
    .orderBy(F.col("n_claim_type_flood").desc(), F.col("n_status_paid").desc())
)
