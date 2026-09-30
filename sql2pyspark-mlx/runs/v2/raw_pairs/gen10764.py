from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .select(
        F.col("order_items.id"),
        F.col("order_items.product_id"),
        F.col("order_items.quantity"),
        F.col("orders.shipping_fee"),
        F.col("orders.discount"),
        F.col("orders.store_id"),
        (F.col("order_items.unit_price") - F.col("order_items.quantity")).alias("line_total"),
    )
    .orderBy(F.col("id").desc(), F.col("shipping_fee").desc())
)
