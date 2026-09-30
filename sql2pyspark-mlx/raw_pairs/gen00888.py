from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("revenue") >= 2500) | F.col("product_id").isin([3, 6]))
    .groupBy(F.month("sale_date").alias("sale_month"))
    .agg(F.min("sale_date").alias("min_sale_date"))
    .orderBy("min_sale_date")
)
