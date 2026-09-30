from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 3, 7, 8]))
    .groupBy("store_id", F.quarter("sale_date").alias("sale_quarter"))
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.sum("revenue").alias("total_revenue"),
        F.min("revenue").alias("min_revenue"),
        F.countDistinct("revenue").alias("unique_products"),
    )
    .filter(F.col("unique_products") > 5)
    .drop("unique_products")
    .orderBy("total_revenue", "store_id", "sale_quarter")
    .limit(50)
)
