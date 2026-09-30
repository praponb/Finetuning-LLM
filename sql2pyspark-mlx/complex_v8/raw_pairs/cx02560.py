from pyspark.sql import functions as F

recent_claims = (
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
        "reported_at",
        F.substring(F.trim(F.col("claim_type")), 1, 3).alias("claim_type_prefix"),
        F.to_date(F.col("reported_at")).alias("reported_at_date"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

annotated = (
    recent_claims
    .filter(F.col("claimed_amount").between(14019.8, 32878.5))
    .select(
        "id",
        "reported_at",
        "claim_type_prefix",
        "claimed_amount_bucket",
        F.abs(F.col("approved_amount") - 2349.3).alias("approved_amount_distance"),
        F.quarter(F.col("reported_at_date")).alias("reported_at_date_quarter"),
    )
)

result = (
    annotated
    .filter(F.col("approved_amount_distance") >= 17500.0)
    .select(
        "id",
        "claim_type_prefix",
        "approved_amount_distance",
        (F.col("reported_at") + F.expr("INTERVAL 7 DAYS")).alias("reported_at_plus_7d"),
    )
    .orderBy(F.col("approved_amount_distance").desc_nulls_first(), "id")
    .limit(20)
)
