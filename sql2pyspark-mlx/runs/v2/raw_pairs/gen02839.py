from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id")
    .agg(F.round(F.avg("units"), 2).alias("avg_units"))
    .filter(F.col("avg_units") >= 67)
)
