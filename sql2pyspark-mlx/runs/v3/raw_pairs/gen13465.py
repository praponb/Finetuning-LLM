from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7, 8, 10]) & (F.col("revenue") < 3300))
    .groupBy("store_id")
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.min("revenue").alias("min_revenue"),
    )
    .filter(F.col("min_revenue") < 1700)
    .drop("min_revenue")
    .orderBy(F.col("min_sale_date").desc(), "store_id")
    .limit(50)
)
