from pyspark.sql import functions as F

payments_by_order = (
    df_payments
    .groupBy("order_id")
    .agg(F.count("*").alias("cnt"))
)

result = (
    df_orders.alias("o")
    .join(payments_by_order.alias("pbo"), F.col("o.id") == F.col("pbo.order_id"), "left")
    .filter(F.col("pbo.cnt") > 8)
    .select(F.col("o.id"), F.col("o.status"), F.col("pbo.cnt"))
    .orderBy("id")
)
