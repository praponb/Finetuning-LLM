from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
    .filter(F.col("unique_orders") > 7)
    .select("order_id", "unique_orders")
)
