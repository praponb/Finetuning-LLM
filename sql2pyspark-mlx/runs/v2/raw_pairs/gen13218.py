from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .filter(F.col("p.paid_at") < "2023-11-01")
    .groupBy(F.col("o.channel"))
    .agg(
        F.max(F.col("p.amount")).alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
    )
    .orderBy("channel")
)
