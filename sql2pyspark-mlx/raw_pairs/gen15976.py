from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("sale_date") <= "2025-07-29") & (F.col("revenue") < 1000))
    .groupBy("product_id")
    .agg(F.min("sale_date").alias("min_sale_date"))
)
