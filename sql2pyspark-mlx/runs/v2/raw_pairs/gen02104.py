from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "right")
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.amount") >= 1460, "high").when(F.col("o.amount") >= 630, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.sum(F.when(F.col("p.method") == "cash", 1).otherwise(0)).alias("cash_count"))
    .orderBy(F.col("cash_count").desc(), "status", "amount_bucket")
    .limit(5)
)
