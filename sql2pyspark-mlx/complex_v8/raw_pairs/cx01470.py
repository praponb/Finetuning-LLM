from pyspark.sql import functions as F

base = (
    df_claims
    .filter((F.col("policy_id").isNotNull()) & (F.col("claim_type").isin("collision", "flood", "medical")))
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
        F.add_months(F.col("claim_date"), 6).alias("claim_date_plus_6m"),
    )
)

claim_side = (
    base
    .select(
        F.lit("claim").alias("src"),
        "adjuster_id",
        "policy_id",
        F.col("reported_at").alias("happened_at"),
    )
)

claim_side_2 = (
    df_claims
    .select(
        F.lit("claim_2").alias("src"),
        F.col("id").alias("adjuster_id"),
        F.col("adjuster_id").alias("policy_id"),
        F.col("reported_at").alias("happened_at"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.lit("claim_3").alias("src"),
        "adjuster_id",
        F.col("id").alias("policy_id"),
        F.col("reported_at").alias("happened_at"),
    )
)

result = (
    claim_side
    .unionByName(claim_side_2)
    .unionByName(claim_rows)
)
