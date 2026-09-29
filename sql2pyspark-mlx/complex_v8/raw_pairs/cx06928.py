from pyspark.sql import functions as F

base_claims = (
    df_claims
    .filter((F.col("policy_id") >= 12) & (F.col("claim_date") >= F.lit("2024-04-14").cast("date")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        "reported_at",
        F.concat_ws(" / ", F.col("status"), F.col("claim_type")).alias("status_claim_type_label"),
    )
)

claim_side = (
    base_claims
    .filter((F.col("policy_id") <= 19) & (F.hour(F.col("reported_at")).between(7, 10)))
    .select(
        F.lit("claim").alias("origin"),
        F.col("policy_id").alias("key_id"),
        "claimed_amount",
    )
)

claim_rows = df_claims.select(F.lit("claim_2").alias("origin"), "policy_id", "claimed_amount")

holder_side = df_policyholders.select(F.lit("holder").alias("origin"), "id", "risk_score")

result = (
    claim_side
    .union(claim_rows)
    .union(holder_side)
)
