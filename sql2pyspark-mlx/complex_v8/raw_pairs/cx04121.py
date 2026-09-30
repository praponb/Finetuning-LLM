from pyspark.sql import functions as F

scoped = (
    df_claims
    .filter(F.col("claim_type").isNotNull())
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

claim_rows = scoped.select(F.lit("claim").alias("origin"), F.col("id").alias("qty"), "claim_type")

claim_rows_2 = df_claims.select(F.lit("claim_2").alias("origin"), "policy_id", "claim_type")

result = claim_rows.union(claim_rows_2)
