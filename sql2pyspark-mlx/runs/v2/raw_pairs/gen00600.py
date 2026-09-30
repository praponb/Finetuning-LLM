from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("orders.id") == F.col("order_items.order_id"), "left")
    .filter(F.col("order_items.product_id").isin([11, 21, 28]))
    .select(
        F.col("order_items.unit_price"),
        F.col("orders.amount"),
        F.col("orders.discount"),
    )
    .orderBy("unit_price")
)
