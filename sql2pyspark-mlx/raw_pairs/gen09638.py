from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("o.store_id").isNull())
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(F.count(F.col("p.id")).alias("payment_count"))
    .filter(F.col("payment_count") >= 5)
)
