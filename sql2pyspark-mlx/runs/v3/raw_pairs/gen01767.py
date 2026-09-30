from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "outer")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .select(
        F.col("order_items.unit_price"),
        F.col("orders.channel"),
        F.col("orders.amount"),
        F.col("stores.sqft"),
        F.col("orders.amount").cast("int").alias("amount_int"),
    )
)
