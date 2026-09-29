from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.col("o.discount") <= 0.4)
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.sum(F.when(F.col("p.method") == "card", 1).otherwise(0)).alias("card_count"),
    )
    .filter(F.col("payment_count") >= 3)
    .orderBy(F.col("id").desc(), "status")
    .limit(3)
)
