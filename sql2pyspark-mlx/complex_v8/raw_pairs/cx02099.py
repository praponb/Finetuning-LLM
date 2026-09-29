from pyspark.sql import functions as F

prepared = (
    df_policyholders
    .filter(F.col("risk_score").between(0.2, 0.6))
    .select(
        "id",
        "holder_name",
        "region",
        "risk_score",
        "joined_on",
    )
)

holder_side = prepared.select("id")

claim_rows = (
    df_claims
    .filter((F.col("claim_type") != "delay") & (F.col("policy_id").isNotNull()))
    .select("policy_id")
)

result = holder_side.subtract(claim_rows)
