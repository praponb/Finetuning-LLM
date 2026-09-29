from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter(F.col("adjuster_id") >= 7)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "status",
        F.substring(F.trim(F.col("status")), 1, 4).alias("status_prefix"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("reported_at")).alias("days_since_reported_at"),
        F.lower(F.regexp_replace(F.trim(F.col("status")), " ", "_")).alias("status_slug"),
    )
)

screened = (
    base_claims
    .crossJoin(base_claims.agg(F.avg("days_since_reported_at").alias("overall_avg_days_since_reported_at")))
    .filter(F.col("days_since_reported_at") > F.col("overall_avg_days_since_reported_at"))
    .drop("overall_avg_days_since_reported_at")
)

result = (
    screened
    .groupBy("claim_type", "claim_date")
    .pivot("status", ["IN_REVIEW", "APPROVED", "REJECTED", "OPEN"])
    .agg(F.sum("days_since_reported_at"))
)
