from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .select(
        F.col("status").alias("label"),
        F.col("start_date").alias("event_date"),
        F.lit("policy").alias("source"),
    )
)

claim_side = df_claims.select("claim_type", "claim_date", F.lit("claim").alias("source"))

unioned = policy_rows.union(claim_side)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_date").alias("latest_event_date"),
    )
    .orderBy("source")
)
