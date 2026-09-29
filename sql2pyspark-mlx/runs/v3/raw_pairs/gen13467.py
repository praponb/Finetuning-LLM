from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "inner")
    .groupBy(F.col("orders.store_id"))
    .agg(
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
        F.countDistinct(F.col("order_items.quantity")).alias("unique_products"),
    )
)
