from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .filter(F.col("p.order_id").isNotNull())
    .join(df_orders.filter(F.col("amount") < 1410).alias("o"), F.col("p.order_id") == F.col("o.id"), "left_anti")
    .select(F.col("p.id"), F.col("p.order_id"), F.col("p.paid_at"), F.col("p.status"))
    .orderBy("status", "paid_at")
)
