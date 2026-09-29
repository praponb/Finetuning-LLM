from pyspark.sql import functions as F

policy_side = df_policies.select("id")

adjuster_side = df_adjusters.select(F.col("level").alias("id"))

result = policy_side.subtract(adjuster_side)
