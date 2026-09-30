from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("shipping_fee") <= 16).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("p.status") == "FAILED")
    .select(F.col("p.id"), F.col("p.method"), F.col("p.order_id"))
)
