from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .groupBy(F.col("o.channel"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "FAILED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_failed"),
        F.avg(F.col("p.amount")).alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
