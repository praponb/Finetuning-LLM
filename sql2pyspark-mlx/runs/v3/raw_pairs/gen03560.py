from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .agg(
        F.min("sale_date").alias("min_sale_date"),
        F.count("*").alias("row_count"),
        F.count("revenue").alias("revenue_count"),
    )
    .orderBy(F.col("revenue_count").desc())
)
