from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_products.alias("products"), F.col("order_items.product_id") == F.col("products.id"), "inner")
    .groupBy(
        F.when(F.col("orders.discount") >= 0.32, "high").when(F.col("orders.discount") >= 0.1, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.sum((F.col("order_items.quantity") * F.col("order_items.unit_price"))).alias("revenue"),
    )
    .filter(F.col("revenue") > 42)
    .orderBy("discount_bucket")
)
