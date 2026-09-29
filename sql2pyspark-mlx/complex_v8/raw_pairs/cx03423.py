from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .select(
        F.col("adjuster_id").alias("key_id"),
        F.col("approved_amount").alias("metric"),
        "claim_date",
    )
)

holder_rows = (
    df_policyholders
    .filter((F.col("holder_name").isNotNull()) & (F.col("region").isin("West", "Central", "East")))
    .select("id", "risk_score", "joined_on")
)

claim_rows_2 = df_claims.select("id", "claimed_amount", "claim_date")

unioned = (
    claim_rows
    .union(holder_rows)
    .union(claim_rows_2)
)

result = (
    unioned
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("metric").alias("total_metric"),
        F.max("claim_date").alias("latest_claim_date"),
    )
    .orderBy("key_id")
)
