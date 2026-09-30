from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter((F.col("start_date") >= F.lit("2024-10-22").cast("date")) & (F.col("premium") > 2111.3))
    .select(
        F.lit("policy").alias("src"),
        F.col("holder_id").alias("qty"),
        F.col("premium").alias("metric"),
    )
)

holder_side = df_policyholders.select(F.lit("holder").alias("src"), "id", "risk_score")

claim_rows = (
    df_claims
    .filter(F.col("adjuster_id") <= 5)
    .select(F.lit("claim").alias("src"), "adjuster_id", "claimed_amount")
)

result = (
    policy_side
    .union(holder_side)
    .union(claim_rows)
)
