from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.year(F.col("joined_on")) == 2024)
    .select(F.col("segment").alias("label"))
)

policy_side = df_policies.select(F.col("product").alias("label"))

result = holder_side.intersect(policy_side)
