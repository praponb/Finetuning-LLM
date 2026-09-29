from pyspark.sql import functions as F

src = (
    df_store_sales
    .filter(F.col("store_id").isin([4, 5, 6]) | (F.col("sale_date") < "2025-09-16"))
)

store_sales_stats = (
    src
    .groupBy("product_id", "store_id")
    .agg(
        F.max("units").alias("max_units"),
        F.sum("revenue").alias("total_revenue"),
    )
)

result = store_sales_stats.select("product_id", "store_id", "max_units", "total_revenue")
