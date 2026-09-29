from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .filter(F.hour(F.col("p.paid_at")).between(12, 15))
    .groupBy(
        F.when(F.col("o.discount") >= 0.4, "high").when(F.col("o.discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
        F.col("o.customer_id"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .orderBy(F.col("discount_bucket").desc())
)
