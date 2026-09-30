from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.max("sale_date").alias("latest_sale_date"),
        F.count("*").alias("store_sale_count"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy(F.col("product_id").desc())
)
