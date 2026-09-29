from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.status") != "SHIPPED")
    .select(
        F.col("o.customer_id"),
        F.col("o.shipping_fee"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.region"),
    )
)
