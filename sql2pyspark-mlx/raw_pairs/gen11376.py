from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_payments.alias("p"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.year(F.col("o.order_date")) >= 2025)
    .groupBy(F.col("o.status"))
    .agg(
        F.count(F.col("p.id")).alias("payment_count"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy("payment_count", F.col("avg_amount").desc(), "status")
    .limit(10)
)
