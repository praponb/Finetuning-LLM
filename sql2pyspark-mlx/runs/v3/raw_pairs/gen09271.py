from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isin([34, 38]))
    .groupBy("order_id")
    .agg(F.countDistinct("quantity").alias("unique_orders"))
    .filter(F.col("unique_orders") >= 8)
    .select("order_id", "unique_orders")
    .orderBy(F.col("unique_orders").desc())
)
