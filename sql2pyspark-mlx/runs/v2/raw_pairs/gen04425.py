from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 1400)
    .groupBy("store_id")
    .agg(F.avg("units").alias("avg_units"))
    .filter(F.col("avg_units") >= 36)
)
