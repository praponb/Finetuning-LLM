from pyspark.sql import functions as F

policy_rows = df_policies.select("id", "start_date")

policy_rows_2 = (
    df_policies
    .select(F.col("holder_id").alias("id"), F.col("end_date").alias("start_date"))
)

result = policy_rows.unionByName(policy_rows_2)
