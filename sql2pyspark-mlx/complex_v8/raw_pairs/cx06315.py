from pyspark.sql import functions as F

policy_rows = df_policies.select(F.col("holder_id").alias("entity_id"))

holder_rows = df_policyholders.select(F.col("id").alias("entity_id"))

result = policy_rows.subtract(holder_rows)
