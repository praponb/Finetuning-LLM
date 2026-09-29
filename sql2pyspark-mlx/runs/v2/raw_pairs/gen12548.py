from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 3)
    .groupBy("product_id")
    .agg(
        F.min("sale_date").alias("first_sale_date"),
        F.max("revenue").alias("max_revenue"),
    )
    .orderBy("max_revenue", F.col("product_id").desc())
)
