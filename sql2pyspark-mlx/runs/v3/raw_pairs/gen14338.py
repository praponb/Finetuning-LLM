from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(F.count(F.col("p.id")).alias("payment_count"))
    .orderBy("status", F.col("id").desc())
    .limit(1)
)
