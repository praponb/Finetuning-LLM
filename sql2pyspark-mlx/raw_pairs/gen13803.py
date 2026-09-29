from pyspark.sql import functions as F

result = (
    df_payments.alias("payments")
    .join(df_orders.alias("orders"), F.col("payments.order_id") == F.col("orders.id"), "left")
    .filter(F.col("payments.amount") >= 1340)
    .groupBy(
        F.when(F.col("orders.discount") >= 0.32, "high").when(F.col("orders.discount") >= 0.07, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("payments.status") == "REFUNDED", 1).otherwise(0))) / F.count("*")), 1).alias("pct_refunded"),
        F.count(F.when(F.col("payments.method") == "card", 1)).alias("card_count"),
    )
    .orderBy("discount_bucket")
)
