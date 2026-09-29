from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .filter(F.col("o.customer_id").isin([23, 28, 38]))
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 31, "high").when(F.col("o.shipping_fee") >= 7, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count(F.col("p.amount")).alias("amount_count"))
    .filter(F.col("amount_count") >= 5)
)
