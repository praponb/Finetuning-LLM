from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .select(
        F.col("o.id"),
        F.col("o.order_date"),
        F.col("o.store_id"),
        F.col("o.shipping_fee"),
        F.col("s.region"),
        F.col("s.city"),
        F.col("s.sqft"),
    )
)
