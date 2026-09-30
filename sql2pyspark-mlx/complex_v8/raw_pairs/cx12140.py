from pyspark.sql import functions as F

policy_side = (
    df_policies
    .select(F.col("id").alias("ref_id"), F.col("premium").alias("value"), "holder_id")
)

claim_side = df_claims.select("id", "claimed_amount", "adjuster_id")

all_events = (
    policy_side
    .union(claim_side)
    .distinct()
)

result = (
    all_events
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("ref_id")
)
