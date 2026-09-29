from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") >= 2800)
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("revenue").alias("revenue_count"),
    )
    .filter(F.col("unique_products") >= 4)
    .select("store_id", "unique_products", "revenue_count")
    .orderBy(F.col("unique_products").desc())
)
