from pyspark.sql import functions as F

recent_claims = (
    df_claims
    .filter(~F.col("status").isin("REJECTED", "APPROVED"))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claim_type",
        "claimed_amount",
        "approved_amount",
        "status",
        F.dayofweek(F.col("reported_at")).alias("reported_at_dayofweek"),
    )
)

claim_rows = (
    recent_claims
    .select(
        F.lit("claim").alias("origin"),
        "reported_at_dayofweek",
        F.col("status").alias("label"),
    )
)

holder_side = df_policyholders.select(F.lit("holder").alias("origin"), "id", "segment")

result = (
    claim_rows
    .union(holder_side)
    .orderBy(F.col("label").desc(), "origin")
)
