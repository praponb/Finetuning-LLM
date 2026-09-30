from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("cost") >= 55) & (F.year("shipped_date") >= 2025))
    .groupBy(F.quarter("delivered_date").alias("delivered_quarter"))
    .agg(F.sum("cost").alias("total_cost"))
    .filter(F.col("total_cost") >= 180)
)
