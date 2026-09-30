from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2100, 3100))
    .groupBy("store_id")
    .agg(
        F.countDistinct("revenue").alias("unique_products"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("unique_products") < 1)
    .orderBy(F.col("unique_products").desc(), "store_id")
    .limit(50)
)
