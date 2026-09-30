from pyspark.sql import functions as F

policy_side = (
    df_policies
    .filter(~F.col("product").isin("LIFE", "HOME"))
    .select(
        F.col("product").alias("name"),
        F.col("start_date").alias("day"),
        "holder_id",
        F.lit("policy").alias("source"),
    )
)

policy_rows = (
    df_policies
    .filter(F.col("start_date").between(F.lit("2024-08-27").cast("date"), F.lit("2025-10-13").cast("date")))
    .select(
        F.col("product").alias("name"),
        F.col("start_date").alias("day"),
        "holder_id",
        F.lit("policy_2").alias("source"),
    )
)

claim_rows = (
    df_claims
    .select(
        F.col("claim_type").alias("name"),
        F.col("claim_date").alias("day"),
        F.col("id").alias("holder_id"),
        F.lit("claim").alias("source"),
    )
)

stacked = (
    policy_side
    .unionByName(policy_rows)
    .unionByName(claim_rows)
)

result = (
    stacked
    .groupBy("source")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("holder_id").alias("total_holder_id"),
        F.max("day").alias("latest_day"),
    )
    .orderBy("source")
)
