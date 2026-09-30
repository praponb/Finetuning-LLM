from pyspark.sql import functions as F

claim_rows = (
    df_claims
    .filter(F.col("reported_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        F.col("id").alias("key_id"),
        F.col("claim_date").alias("day"),
        F.col("claimed_amount").alias("value"),
    )
)

policy_rows = df_policies.select("holder_id", "start_date", "premium")

stacked = (
    claim_rows
    .union(policy_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("key_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("value").alias("total_value"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("key_id")
)
