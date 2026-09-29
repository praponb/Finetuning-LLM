from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("revenue") > 3500)
    .withColumn("revenue_per_unit", (F.col("units") - F.col("revenue")))
)

agg = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.min("revenue_per_unit").alias("min_revenue_per_unit"))
)

result = agg.filter(F.col("min_revenue_per_unit") >= 38)
