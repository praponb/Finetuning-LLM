from pyspark.sql import functions as F

filtered = (
    df_claims
    .filter(F.col("adjuster_id").isNotNull())
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "reported_at",
    )
)

claim_side = (
    filtered
    .select(
        F.col("id").alias("qty"),
        F.col("reported_at").alias("happened_at"),
        F.lit("claim").alias("source"),
    )
)

claim_rows = df_claims.select("policy_id", "reported_at", F.lit("claim_2").alias("source"))

result = (
    claim_side
    .union(claim_rows)
    .distinct()
    .orderBy("happened_at")
)
