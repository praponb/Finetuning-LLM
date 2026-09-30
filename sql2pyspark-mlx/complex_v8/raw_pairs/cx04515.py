from pyspark.sql import functions as F

claim_side = df_claims.select("claim_type", "status", F.col("claimed_amount").alias("value"))

claim_side_2 = df_claims.select("claim_type", "status", F.col("claimed_amount").alias("value"))

all_events = claim_side.unionByName(claim_side_2)

result = (
    all_events
    .groupBy("claim_type")
    .agg(F.count("*").alias("n_rows"), F.sum("value").alias("total_value"))
    .orderBy("claim_type")
)
