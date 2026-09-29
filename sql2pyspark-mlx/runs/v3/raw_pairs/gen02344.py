from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left_anti")
    .filter(F.col("o.store_id") == 37)
    .select(F.col("o.id"), F.col("o.status"), F.col("o.shipping_fee"))
    .orderBy(F.col("shipping_fee").desc())
)
