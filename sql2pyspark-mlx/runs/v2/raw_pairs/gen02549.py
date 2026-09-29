from pyspark.sql import functions as F

result = (
    df_order_items.alias("order_items")
    .join(df_orders.alias("orders"), F.col("order_items.order_id") == F.col("orders.id"), "inner")
    .join(df_stores.alias("stores"), F.col("orders.store_id") == F.col("stores.id"), "inner")
    .filter(F.col("orders.store_id").isin([7, 32]))
    .groupBy(
        F.month(F.col("stores.opened_date")).alias("opened_month"),
        F.col("stores.city"),
    )
    .agg(
        F.max(F.col("order_items.quantity")).alias("max_quantity"),
        F.sum((F.col("order_items.unit_price") * F.col("order_items.quantity"))).alias("revenue"),
    )
)
