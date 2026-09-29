from pyspark.sql import functions as F

cleaned_claims = (
    df_claims
    .filter((~F.col("claim_type").isin("delay", "collision")) & (F.col("adjuster_id") >= 2))
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

claim_side = cleaned_claims.select(F.col("status").alias("label"), "claim_type")

holder_side = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("D%")) & (~F.col("segment").isin("RETAIL", "VIP")))
    .select("segment", "holder_name")
)

result = (
    claim_side
    .union(holder_side)
    .orderBy(F.col("claim_type").desc(), "label")
)
