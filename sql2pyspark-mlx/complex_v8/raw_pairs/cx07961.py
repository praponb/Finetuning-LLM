from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter((F.col("policy_id") >= 20) & (F.col("claim_type") == "theft"))
    .select(
        F.lit("claim").alias("source"),
        "claim_type",
        F.col("reported_at").alias("event_time"),
    )
)

claim_rows = df_claims.select(F.lit("claim_2").alias("source"), "claim_type", "reported_at")

unioned = claim_side.union(claim_rows)

result = (
    unioned
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("event_time").alias("latest_event_time"),
    )
    .orderBy("source")
)
