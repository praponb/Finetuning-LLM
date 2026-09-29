from pyspark.sql import functions as F

policy_side = df_policies.select("product", F.col("id").alias("ref_id"))

claim_side = (
    df_claims
    .filter(F.col("claim_date").between(F.lit("2024-04-14").cast("date"), F.lit("2025-06-24").cast("date")))
    .select(F.col("status").alias("product"), F.col("id").alias("ref_id"))
)

all_events = (
    policy_side
    .unionByName(claim_side)
    .distinct()
)

result = (
    all_events
    .groupBy("product")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("product")
)
