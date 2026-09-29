from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.filter(F.col("channel") == "partner").alias("o"), F.col("p.order_id") == F.col("o.id"), "left_semi")
    .filter(F.hour(F.col("p.paid_at")).between(8, 14))
    .select(F.col("p.id"), F.col("p.method"), F.col("p.status"), F.col("p.paid_at"))
)
