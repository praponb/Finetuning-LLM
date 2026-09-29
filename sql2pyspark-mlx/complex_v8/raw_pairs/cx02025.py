from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("claimed_amount").between(3192.1, 42838.9)) & (F.col("claim_date") < F.lit("2025-01-24").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "reported_at",
    )
)

claim_side = (
    base
    .filter((F.month(F.col("reported_at")).isin(1, 3)) & (F.col("policy_id") >= 21))
    .select("id", "reported_at")
)

claim_side_2 = df_claims.select(F.col("adjuster_id").alias("id"), "reported_at")

result = (
    claim_side
    .unionByName(claim_side_2)
    .orderBy("reported_at")
)
