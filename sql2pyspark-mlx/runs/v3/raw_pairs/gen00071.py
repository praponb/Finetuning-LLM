from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(F.col("o.id"), F.col("o.channel"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.count(F.col("p.amount")).alias("amount_count"),
    )
    .filter(F.col("payment_count") >= 3)
    .orderBy("payment_count", "id", "channel")
    .limit(50)
)
