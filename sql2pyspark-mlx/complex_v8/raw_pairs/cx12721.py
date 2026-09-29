from pyspark.sql import functions as F

policy_side = df_policies.select(F.col("holder_id").alias("qty"))

policy_side_2 = df_policies.select("holder_id")

result = (
    policy_side
    .intersect(policy_side_2)
    .orderBy("qty")
)
