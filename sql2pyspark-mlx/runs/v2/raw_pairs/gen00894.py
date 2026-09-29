from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(
        F.when(F.col("o.shipping_fee") >= 41, "high").when(F.col("o.shipping_fee") >= 17, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.max(F.col("p.paid_at")).alias("last_paid_at"))
    .orderBy("last_paid_at", "shipping_fee_bucket")
)
