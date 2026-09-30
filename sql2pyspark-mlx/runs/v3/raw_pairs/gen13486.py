from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.discount").between(0.18, 0.32))
    .select(
        F.col("s.store_name"),
        F.col("s.city"),
        F.col("s.opened_date"),
        F.col("o.order_date"),
        F.col("o.shipping_fee"),
        F.col("o.channel"),
    )
    .orderBy("shipping_fee")
)
