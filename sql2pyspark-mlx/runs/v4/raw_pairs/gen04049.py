from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .select(F.col("p.order_id"), F.col("p.status"), F.col("o.shipping_fee"))
    .orderBy(F.col("shipping_fee").desc())
)
