from pyspark.sql import functions as F

adjuster_side = df_adjusters.select(F.col("id").alias("key_id"))

policy_rows = df_policies.select("holder_id")

result = adjuster_side.intersect(policy_rows)
