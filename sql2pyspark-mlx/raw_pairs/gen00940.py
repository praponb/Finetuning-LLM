from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .filter(F.col("oi.order_id").isin([11, 17]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.coalesce(F.col("oi.unit_price"), F.lit(0)).alias("unit_price_filled"),
    )
)
