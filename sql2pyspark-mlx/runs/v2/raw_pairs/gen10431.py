from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .filter(F.col("order_items.product_id") == 6)
    .groupBy(
        F.col("products.brand"),
        F.date_trunc("MONTH", F.col("products.launch_date")).alias("launch_month"),
    )
    .agg(
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
        F.max(F.col("order_items.quantity")).alias("max_quantity"),
    )
    .filter(F.col("max_quantity") >= 6.8)
)
