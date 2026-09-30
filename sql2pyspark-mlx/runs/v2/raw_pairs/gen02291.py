from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg(
        F.min("revenue").alias("min_revenue"),
        F.max("sale_date").alias("last_sale_date"),
    )
    .filter(F.col("min_revenue") >= 3700)
)
