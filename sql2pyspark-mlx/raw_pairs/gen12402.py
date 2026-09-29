from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_products.alias("products"), F.col("products.id") == F.col("order_items.product_id"), "left")
    .filter(F.col("orders.status").isin(["PENDING", "RETURNED"]))
    .groupBy(F.col("products.category"))
    .agg(
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
    )
)
