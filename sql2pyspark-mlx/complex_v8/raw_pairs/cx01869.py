from pyspark.sql import functions as F

holder_side = df_policyholders.select(F.col("id").alias("entity_id"), "risk_score")

holder_side_2 = (
    df_policyholders
    .filter(F.col("risk_score") >= 0.3)
    .select("id", "risk_score")
)

result = (
    holder_side
    .union(holder_side_2)
    .orderBy("risk_score")
)
