from pyspark.sql import functions as F

policy_side = df_policies.select(F.col("id").alias("entity_id"))

holder_side = df_policyholders.select("id")

result = (
    policy_side
    .intersect(holder_side)
    .orderBy("entity_id")
)
