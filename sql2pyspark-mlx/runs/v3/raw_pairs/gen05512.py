from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .groupBy(
        F.col("orders.channel"),
        F.when(F.col("orders.status").isin(["COMPLETED", "SHIPPED"]), "core").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(F.stddev(F.col("order_items.unit_price")), 2).alias("stddev_unit_price"),
        F.avg(F.col("order_items.quantity")).alias("avg_quantity"),
    )
    .orderBy(F.col("stddev_unit_price").desc(), "channel")
)
