from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("oi.quantity") < 5)
    .groupBy(F.col("s.region"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        (F.max(F.col("oi.quantity")) - F.min(F.col("oi.quantity"))).alias("quantity_range"),
    )
    .orderBy("unit_price_range", "region")
    .limit(10)
)
