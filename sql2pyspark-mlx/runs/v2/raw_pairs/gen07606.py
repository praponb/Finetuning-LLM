from pyspark.sql import functions as F

order_summary = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = (
    df_orders.alias("o")
    .join(order_summary.alias("os"), F.col("o.id") == F.col("os.order_id"), "left")
    .filter(F.col("os.unique_products") >= 7)
    .select(F.col("o.amount"), F.col("o.channel"), F.col("os.unique_products"))
    .orderBy("amount", F.col("unique_products").desc())
)
