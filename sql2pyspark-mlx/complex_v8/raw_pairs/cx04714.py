from pyspark.sql import functions as F

holder_rows = (
    df_policyholders
    .select(F.col("id").alias("entity_id"), F.col("region").alias("category"))
)

policy_side = df_policies.select("holder_id", "status")

holder_rows_2 = (
    df_policyholders
    .filter(F.col("segment").isin("VIP", "SME"))
    .select("id", "segment")
)

result = (
    holder_rows
    .union(policy_side)
    .union(holder_rows_2)
    .orderBy("category")
)
