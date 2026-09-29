from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
        F.when(F.col("o.discount") >= 0.3, "high").when(F.col("o.discount") >= 0.09, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.round(F.avg(F.col("p.amount")), 2).alias("avg_amount"),
    )
    .orderBy("avg_amount")
)
