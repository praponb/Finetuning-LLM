from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "left")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .groupBy(
        F.col("products.category"),
        F.when(F.col("products.stock_qty") >= 380, "high").when(F.col("products.stock_qty") >= 113, "mid").otherwise("low").alias("stock_qty_bucket"),
    )
    .agg(
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
    )
)
