from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([11, 35, 39, 40]))
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
    .orderBy("unique_orders")
)
