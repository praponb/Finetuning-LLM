from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") <= 340) & (F.col("quantity") == 5))
    .groupBy("order_id")
    .agg(F.count("*").alias("total_count"))
)
