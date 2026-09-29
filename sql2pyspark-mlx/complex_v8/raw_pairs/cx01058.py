from pyspark.sql import functions as F

holder_rows = df_policyholders.select("id")

policy_rows = df_policies.select("id")

result = (
    holder_rows
    .intersect(policy_rows)
    .orderBy(F.col("id").desc())
)
