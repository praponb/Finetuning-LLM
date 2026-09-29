from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.col("claimed_amount").between(17585.2, 44921.3)) & (F.to_date(F.col("reported_at")) < F.lit("2025-02-18").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("claim_date")).alias("days_since_claim_date"),
        F.substring(F.trim(F.col("claim_type")), 1, 4).alias("claim_type_prefix"),
        F.concat_ws("-", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

derived = (
    prepared_claims
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "approved_amount",
        "status",
        "reported_at",
        "days_since_claim_date",
        "claim_type_prefix",
        "status_claim_type_label",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        (F.floor(F.col("claimed_amount") / 10000) * 10000).alias("claimed_amount_bucket"),
    )
)

result = (
    derived
    .groupingSets(
        [["policy_id", "status"], ["policy_id"], ["status"], []],
        "policy_id", "status",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sum("claimed_amount_bucket").alias("total_claimed_amount_bucket"),
    )
    .orderBy(F.col("policy_id").asc_nulls_last(), F.col("status").asc_nulls_last())
)
