from pyspark.sql import functions as F

scoped_claims = (
    df_claims
    .filter((F.col("adjuster_id") >= 3) & (~F.col("status").isin("IN_REVIEW", "OPEN")))
    .select(
        "id",
        "policy_id",
        "adjuster_id",
        "claim_date",
        "claimed_amount",
        "approved_amount",
        "status",
    )
)

claim_side = (
    scoped_claims
    .filter((F.col("status").isin("PAID", "REJECTED", "IN_REVIEW")) & (F.col("adjuster_id").isNotNull()))
    .select("policy_id")
)

holder_side = (
    df_policyholders
    .filter((F.length(F.trim(F.col("holder_name"))) >= 5) & (F.col("region") == "East"))
    .select("id")
)

result = (
    claim_side
    .intersectAll(holder_side)
    .orderBy(F.col("policy_id").desc())
)
