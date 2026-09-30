from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("oi.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .filter(F.col("oi.unit_price") <= 350)
    .groupBy(F.col("s.city"), F.col("s.region"))
    .agg(
        F.round(F.avg(F.col("oi.quantity")), 2).alias("avg_quantity"),
        F.countDistinct(F.col("oi.unit_price")).alias("unique_products"),
    )
    .orderBy("city")
)
