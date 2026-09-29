from pyspark.sql import functions as F

prepared = (
    df_claims
    .filter((F.month(F.col("reported_at")).isin(2, 3)) & (F.col("approved_amount") >= 0.0))
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
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
        F.least(F.col("approved_amount"), F.col("claimed_amount")).alias("least_approved_amount_claimed_amount"),
        F.concat(F.col("status"), F.lit("#"), F.col("adjuster_id").cast("string")).alias("status_ref"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.max("claimed_amount").alias("max_claim_claimed_amount"),
        F.count("*").alias("claim_count"),
    )
)

with_counts = (
    prepared.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        "c.max_claim_claimed_amount",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
    )
)

labeled = (
    with_counts
    .withColumn("days_since_claim_date", F.datediff(F.lit("2025-06-30").cast("date"), F.col("claim_date")))
)

result = (
    labeled
    .groupBy("status", "claim_date")
    .pivot("claim_type", ["delay", "theft", "flood"])
    .agg(F.sum("least_approved_amount_claimed_amount"))
)
