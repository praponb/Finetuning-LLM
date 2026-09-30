from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(~F.col("o.status").isin(["PENDING", "RETURNED"]))
    .groupBy(F.col("o.status"), F.col("o.channel"))
    .agg(
        F.count(F.col("oi.unit_price")).alias("unit_price_count"),
        F.sum((F.col("oi.unit_price") * F.col("oi.quantity"))).alias("revenue"),
    )
    .orderBy("channel", F.col("unit_price_count").desc())
)
