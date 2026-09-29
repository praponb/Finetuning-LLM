from pyspark.sql import functions as F

holder_side = df_policyholders.select(F.col("region").alias("category"), "joined_on")

policy_side = df_policies.select("product", "start_date")

holder_side_2 = (
    df_policyholders
    .filter(F.col("region").isNotNull())
    .select("holder_name", "joined_on")
)

result = (
    holder_side
    .union(policy_side)
    .union(holder_side_2)
)
