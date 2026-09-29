from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("p.status") != "SUCCESS")
    .select(
        F.col("p.id"),
        F.col("p.order_id"),
        F.col("p.status"),
        F.col("o.channel"),
        F.col("o.shipping_fee"),
        F.col("s.opened_date"),
        F.col("s.store_name"),
    )
)
