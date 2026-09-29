from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "left")
    .filter((F.col("orders.discount") <= 0.2) | F.col("order_items.order_id").isin([9, 20, 29]))
    .select(
        F.col("order_items.id"),
        F.col("order_items.unit_price"),
        F.col("orders.store_id"),
        F.col("orders.customer_id"),
        F.col("orders.amount"),
    )
    .orderBy(F.col("store_id").desc(), F.col("id").desc())
)
