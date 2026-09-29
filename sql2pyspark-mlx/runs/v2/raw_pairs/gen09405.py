from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.channel"))
    .agg(
        F.count(F.col("oi.quantity")).alias("quantity_count"),
        F.min(F.col("oi.unit_price")).alias("min_unit_price"),
    )
    .filter(F.col("quantity_count") >= 4)
    .orderBy("channel")
)
