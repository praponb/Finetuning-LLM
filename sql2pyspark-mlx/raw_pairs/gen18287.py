from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .filter(F.col("o.shipping_fee") < 27)
    .select(F.col("p.order_id"), F.col("o.status"), F.col("s.city"))
    .orderBy("city")
)
