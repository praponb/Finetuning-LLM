from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("oi.id"),
        F.col("oi.order_id"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("s.city"),
        F.initcap(F.trim(F.col("s.region"))).alias("region_title"),
    )
)
