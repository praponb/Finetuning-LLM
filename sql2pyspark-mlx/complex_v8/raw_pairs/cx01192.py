from pyspark.sql import functions as F

policy_rows = (
    df_policies
    .filter(F.col("status") == "LAPSED")
    .select("id", F.col("status").alias("category"))
)

holder_side = df_policyholders.select("id", "region")

result = policy_rows.subtract(holder_side)
