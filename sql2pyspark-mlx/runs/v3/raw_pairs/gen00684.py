from pyspark.sql import functions as F

order_stats = (
    df_order_items
    .groupBy("order_id")
    .agg(F.countDistinct("unit_price").alias("unique_products"))
)

result = (
    df_orders.alias("o")
    .join(order_stats.alias("t"), F.col("o.id") == F.col("t.order_id"), "left")
    .filter(F.col("t.unique_products") >= 5)
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.discount"),
        F.col("t.unique_products"),
    )
    .orderBy(F.col("id").desc())
)
