from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.max(F.col("p.amount")).alias("max_amount"),
    )
    .orderBy("payment_count", "id", "channel")
    .limit(10)
)
