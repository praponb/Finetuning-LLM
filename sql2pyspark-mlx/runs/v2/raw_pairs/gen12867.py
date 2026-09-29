from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.sum("revenue").alias("total_revenue"),
    )
    .filter(F.col("store_sale_count") > 5)
    .select("product_id", "store_sale_count", "total_revenue")
    .orderBy("product_id")
    .limit(5)
)
