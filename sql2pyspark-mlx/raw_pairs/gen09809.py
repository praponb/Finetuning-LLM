from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.id"), F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .orderBy(F.col("id").desc())
)
