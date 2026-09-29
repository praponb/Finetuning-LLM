from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(10, 15)) | (F.col("claim_type") == "fire"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
    )
)

claim_side = (
    df_claims
    .filter(F.col("status") == "APPROVED")
    .select(F.col("status").alias("name"), "reported_at")
)

claim_rows = (
    df_claims
    .filter(F.col("claimed_amount") >= 20436.5)
    .select("status", "reported_at")
)

unioned = (
    claim_side
    .union(claim_rows)
    .distinct()
)

result = (
    unioned
    .groupBy("name")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("reported_at").alias("latest_reported_at"),
    )
    .orderBy("name")
)
