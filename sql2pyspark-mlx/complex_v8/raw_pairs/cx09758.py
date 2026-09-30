from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.lower(F.col("holder_name")).like("%li%"))
    .select(F.col("id").alias("key_id"), "holder_name")
)

policy_rows = df_policies.select(F.col("id").alias("key_id"), F.col("product").alias("holder_name"))

result = holder_side.intersect(policy_rows)
