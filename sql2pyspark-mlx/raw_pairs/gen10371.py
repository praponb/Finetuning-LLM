from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "right")
    .filter(F.col("o.channel") == "partner")
    .groupBy(F.col("o.status"))
    .agg(
        (F.max(F.col("oi.unit_price")) - F.min(F.col("oi.unit_price"))).alias("unit_price_range"),
        F.avg(F.col("oi.unit_price")).alias("avg_unit_price"),
    )
    .orderBy("unit_price_range", "status")
)
