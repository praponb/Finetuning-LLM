from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("store_id") == 3) | (F.col("product_id") == 8))
    .groupBy("product_id")
    .agg(
        F.count("*").alias("store_sale_count"),
        F.round(F.avg("revenue"), 2).alias("avg_revenue"),
    )
    .orderBy(
        F.col("store_sale_count").desc(),
        F.col("avg_revenue").desc(),
        "product_id",
    )
    .limit(5)
)
