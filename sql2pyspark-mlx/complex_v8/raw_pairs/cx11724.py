from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(F.col("id").alias("key_id"), "start_date", "status")
)

holder_side = df_policyholders.select("id", "joined_on", "holder_name")

policy_rows_2 = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select("id", "start_date", "product")
)

result = (
    policy_rows
    .union(holder_side)
    .union(policy_rows_2)
)
