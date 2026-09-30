from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter(F.col("orders.discount") >= 0.22)
    .select(
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
        F.col("order_items.unit_price"),
        F.col("orders.amount"),
        F.col("orders.shipping_fee"),
    )
    .orderBy(F.col("product_id").desc(), F.col("quantity").desc())
)
