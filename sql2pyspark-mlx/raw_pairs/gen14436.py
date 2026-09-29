from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("p.paid_at") >= "2024-02-01")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(F.count(F.col("p.id")).alias("payment_count"))
)
