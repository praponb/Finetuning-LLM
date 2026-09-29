from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_semi")
    .filter(F.col("o.discount").between(0.27, 0.36))
    .select(F.col("o.id"), F.col("o.shipping_fee"))
)
