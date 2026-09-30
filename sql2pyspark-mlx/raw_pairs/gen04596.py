from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .groupBy(F.col("o.store_id"))
    .agg(
        F.max(F.col("oi.unit_price")).alias("max_unit_price"),
        F.count("*").alias("cnt"),
    )
    .orderBy("max_unit_price", "cnt")
)
