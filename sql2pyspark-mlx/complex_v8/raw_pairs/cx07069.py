from pyspark.sql import functions as F

prepared_claims = (
    df_claims
    .filter((F.col("policy_id") <= 3) & (F.col("claimed_amount") > 18364.1))
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
        (F.floor(F.col("approved_amount") / 10000) * 10000).alias("approved_amount_bucket"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
    )
)

claim_stats = (
    df_claims
    .groupBy("policy_id")
    .agg(
        F.count(F.when(F.col("status").isin("OPEN", "APPROVED"), 1)).alias("claim_status_count"),
    )
)

profiled = (
    prepared_claims.alias("s")
    .join(claim_stats.alias("c"), F.col("c.policy_id") == F.col("s.policy_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("c.claim_status_count"), F.lit(0)).alias("claim_status_count"),
    )
)

result = (
    profiled
    .groupBy("claim_date", "adjuster_id")
    .pivot("status", ["OPEN", "IN_REVIEW", "PAID", "REJECTED"])
    .agg(F.max("approved_amount"))
    .orderBy("claim_date", "adjuster_id")
)
