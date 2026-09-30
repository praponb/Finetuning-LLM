from pyspark.sql import functions as F

valid_claims = (
    df_claims
    .filter(F.year(F.col("claim_date")) == 2025)
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

claim_rows = (
    valid_claims
    .filter((F.col("status") == "OPEN") & (F.col("policy_id").isNotNull()))
    .select(F.col("id").alias("key_id"), "claim_type")
)

claim_rows_2 = df_claims.select(F.col("id").alias("key_id"), F.col("status").alias("claim_type"))

result = (
    claim_rows
    .exceptAll(claim_rows_2)
    .orderBy("claim_type", "key_id")
)
