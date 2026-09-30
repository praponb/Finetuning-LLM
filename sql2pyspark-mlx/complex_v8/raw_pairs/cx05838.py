from pyspark.sql import functions as F

cleaned_holders = (
    df_policyholders
    .filter(F.upper(F.trim(F.col("holder_name"))).like("A%"))
    .select(
        "id",
        "holder_name",
        "risk_score",
        "joined_on",
        F.abs(F.col("risk_score") - 0.2).alias("risk_score_distance"),
    )
)

holder_rows = cleaned_holders.select(F.col("id").alias("ref_id"), F.col("joined_on").alias("day"))

claim_rows = (
    df_claims
    .filter((F.hour(F.col("reported_at")).between(10, 15)) & (F.col("claimed_amount").between(13635.1, 29985.9)))
    .select("policy_id", "claim_date")
)

result = holder_rows.union(claim_rows)
