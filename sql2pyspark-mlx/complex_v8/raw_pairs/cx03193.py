from pyspark.sql import functions as F

claim_side = df_claims.select("claim_type", "adjuster_id", F.col("reported_at").alias("happened_at"))

claim_rows = (
    df_claims
    .select(
        F.col("status").alias("claim_type"),
        F.col("policy_id").alias("adjuster_id"),
        F.col("reported_at").alias("happened_at"),
    )
)

result = (
    claim_side
    .unionByName(claim_rows)
    .distinct()
    .orderBy(F.col("claim_type").desc(), "adjuster_id")
)
