from pyspark.sql import functions as F

claim_side = (
    df_claims
    .filter(~F.col("claim_type").isin("flood", "delay"))
    .select("id", F.col("policy_id").alias("entity_id"))
)

policy_side = df_policies.select("holder_id", "id")

policy_side_2 = df_policies.select("holder_id", "id")

stacked = (
    claim_side
    .union(policy_side)
    .union(policy_side_2)
)

result = (
    stacked
    .groupBy("id")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("id")
)
