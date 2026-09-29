from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.col("status").alias("category"),
        "claim_date",
        F.col("approved_amount").alias("value"),
    )
)

holder_side = df_policyholders.select("segment", "joined_on", "risk_score")

stacked = (
    claim_rows
    .union(holder_side)
    .distinct()
)

result = (
    stacked
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("value").alias("total_value"),
        F.max("claim_date").alias("latest_claim_date"),
    )
    .orderBy("category")
)
