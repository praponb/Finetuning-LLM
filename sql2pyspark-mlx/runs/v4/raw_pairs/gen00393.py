from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([23, 27, 36]))
    .groupBy("order_id", "product_id")
    .agg(F.count("*").alias("row_count"))
    .filter(F.col("row_count") >= 5)
)
