from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.month(F.col("orders.order_date")).alias("order_month"),
        F.when(F.col("orders.discount") >= 0.41, "high").when(F.col("orders.discount") >= 0.11, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum(F.col("order_items.unit_price")).alias("total_unit_price"),
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
    )
)
