from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter((F.col("units") != 26) & ((F.col("revenue") >= 3300) | (F.col("store_id") == 2)))
)

store_id_summary = (
    recent_store_sales
    .groupBy("store_id")
    .agg(F.sum("revenue").alias("total_revenue"))
)

result = store_id_summary.filter(F.col("total_revenue") >= 23000)
