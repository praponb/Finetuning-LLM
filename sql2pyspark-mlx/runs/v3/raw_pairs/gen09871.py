from pyspark.sql import functions as F

recent_store_sales = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 4, 9]) & (F.col("product_id") == 8))
)

store_sales_stats = (
    recent_store_sales
    .groupBy("product_id")
    .agg(F.sum("units").alias("total_units"))
)

result = (
    store_sales_stats
    .filter(F.col("total_units") >= 130)
    .select("product_id", "total_units")
    .orderBy(F.col("product_id").desc())
)
