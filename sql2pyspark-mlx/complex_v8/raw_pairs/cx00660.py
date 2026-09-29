from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter((F.col("claimed_amount") <= 41760.8) & (F.col("policy_id").isNotNull()))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "status",
        "reported_at",
    )
)

claim_side = (
    recent_claims
    .select(F.col("adjuster_id").alias("entity_id"), F.col("status").alias("name"))
)

claim_side_2 = (
    df_claims
    .filter((F.col("claimed_amount").between(4234.9, 30001.6)) & (F.col("claim_date") >= F.lit("2024-06-07").cast("date")))
    .select("adjuster_id", "status")
)

result = claim_side.union(claim_side_2)
