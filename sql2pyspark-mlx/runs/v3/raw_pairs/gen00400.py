from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "outer")
    .groupBy(F.col("products.brand"))
    .agg(
        F.sum((F.col("order_items.quantity") * F.col("order_items.unit_price"))).alias("revenue"),
        F.round(F.stddev(F.col("order_items.quantity")), 2).alias("stddev_quantity"),
    )
    .filter(F.col("stddev_quantity") > 67)
    .orderBy("revenue", F.col("brand").desc())
    .limit(10)
)
