from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_order_items.alias("oi"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("o.store_id"),
        F.col("oi.quantity"),
        F.coalesce(F.col("o.discount"), F.lit(0)).alias("discount_filled"),
    )
)
