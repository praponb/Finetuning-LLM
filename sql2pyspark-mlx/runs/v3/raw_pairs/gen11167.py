from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("product_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") >= 35)
    .orderBy("product_id")
)
