from pyspark.sql import functions as F

filtered_claims = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(6, 11)) & (F.col("approved_amount").between(0.0, 24718.5)))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "approved_amount",
        "status",
        "reported_at",
    )
)

claim_side = (
    filtered_claims
    .select(
        F.lit("claim").alias("source"),
        F.col("status").alias("category"),
        F.col("claim_type").alias("name"),
        "reported_at",
    )
)

claim_rows = (
    df_claims
    .select(
        F.lit("claim_2").alias("source"),
        F.col("status").alias("category"),
        F.col("claim_type").alias("name"),
        "reported_at",
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.lit("claim_3").alias("source"),
        F.col("status").alias("category"),
        F.col("claim_type").alias("name"),
        "reported_at",
    )
)

result = (
    claim_side
    .unionByName(claim_rows)
    .unionByName(claim_side_2)
)
