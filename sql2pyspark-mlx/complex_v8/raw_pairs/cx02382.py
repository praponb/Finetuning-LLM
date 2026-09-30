from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("status").isNotNull()) & (F.col("claimed_amount") < 29191.2))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
        F.date_format(F.col("claim_date"), "yyyy-MM-dd").alias("claim_date_label"),
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.when(F.col("claimed_amount") >= 29336.0, "HIGH").when(F.col("claimed_amount") >= 17759.3, "MEDIUM").otherwise("LOW").alias("claimed_amount_tier"),
    )
)

claim_stats = (
    df_claims
    .groupBy("adjuster_id")
    .agg(
        F.count("*").alias("claim_count"),
        F.count(F.when(F.col("claim_type") == "delay", 1)).alias("claim_claim_type_count"),
    )
)

with_stats = (
    base_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.adjuster_id") == F.col("s.adjuster_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_count"), F.lit(0)).alias("claim_count"),
        F.coalesce(F.col("c.claim_claim_type_count"), F.lit(0)).alias("claim_claim_type_count"),
    )
)

scored = (
    with_stats
    .filter(F.col("claim_date_label").isNotNull())
    .withColumn("claim_count_filled", F.coalesce(F.col("claim_count"), F.lit(0)))
)

result = (
    scored
    .filter(F.col("status") == "IN_REVIEW")
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        "approved_amount_bucket",
        "claimed_amount_tier",
        "claim_count",
        "claim_count_filled",
    )
)
