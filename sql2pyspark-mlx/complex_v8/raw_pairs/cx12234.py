from pyspark.sql import functions as F

policy_side = df_policies.select(F.col("id").alias("key_id"))

policy_rows = df_policies.select(F.col("holder_id").alias("key_id"))

result = (
    policy_side
    .intersect(policy_rows)
    .orderBy(F.col("key_id").desc())
)
