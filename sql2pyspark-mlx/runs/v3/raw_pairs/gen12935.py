from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(
        F.col("c.is_active"),
        F.when(F.col("c.credit_limit") >= 16700, "high").when(F.col("c.credit_limit") >= 2700, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min(F.col("o.shipping_fee")).alias("min_shipping_fee"),
        F.round(F.avg(F.col("o.discount")), 2).alias("avg_discount"),
    )
)
