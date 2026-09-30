from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "right")
    .select(
        F.col("p.order_id"),
        F.col("o.store_id"),
        F.col("s.store_name"),
        F.col("s.city"),
    )
    .orderBy("city")
)
