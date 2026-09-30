from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("o.id") == F.col("oi.order_id"), "inner")
    .filter((F.col("o.store_id") == 10) | (F.col("o.status") == "RETURNED"))
    .select(F.col("o.status"), F.col("oi.product_id"), F.col("oi.unit_price"))
)
