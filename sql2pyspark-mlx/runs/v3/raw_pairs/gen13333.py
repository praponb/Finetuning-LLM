from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "inner")
    .groupBy(
        F.col("o.status"),
        F.when(F.col("o.discount") >= 0.35, "high").when(F.col("o.discount") >= 0.18, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.min(F.col("p.paid_at")).alias("first_paid_at"),
        F.sum(F.col("p.amount")).alias("total_amount"),
    )
    .orderBy(F.col("total_amount").desc(), F.col("discount_bucket").desc(), "status")
    .limit(20)
)
