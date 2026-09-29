from pyspark.sql import functions as F

base_holders = (
    df_policyholders
    .filter(F.lower(F.col("holder_name")).like("%a %"))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
        "segment",
    )
)

holder_side = (
    base_holders
    .filter(F.col("joined_on").between(F.lit("2024-02-04").cast("date"), F.lit("2025-10-01").cast("date")))
    .select("id", "joined_on")
)

claim_side = df_claims.select("policy_id", "claim_date")

policy_rows = (
    df_policies
    .filter(F.col("holder_id") <= 10)
    .select("holder_id", "end_date")
)

result = (
    holder_side
    .union(claim_side)
    .union(policy_rows)
    .orderBy(F.col("id").desc())
)
