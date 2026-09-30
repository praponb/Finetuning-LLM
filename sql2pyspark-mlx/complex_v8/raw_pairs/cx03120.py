from pyspark.sql import functions as F

holder_side = (
    df_policyholders
    .filter(F.col("region") != "East")
    .select(F.lit("holder").alias("record_type"), "id", "risk_score")
)

holder_rows = (
    df_policyholders
    .filter((F.col("joined_on") >= F.lit("2024-06-18").cast("date")) & (F.col("risk_score") >= 0.1))
    .select(F.lit("holder_2").alias("record_type"), "id", "risk_score")
)

result = (
    holder_side
    .union(holder_rows)
    .orderBy("risk_score", "id")
)
