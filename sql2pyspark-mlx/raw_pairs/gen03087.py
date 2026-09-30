from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 5, 9]))
    .groupBy("store_id", "product_id")
    .agg((F.max("revenue") - F.min("revenue")).alias("revenue_range"))
    .filter(F.col("revenue_range") < 51)
    .select("store_id", "product_id", "revenue_range")
)
