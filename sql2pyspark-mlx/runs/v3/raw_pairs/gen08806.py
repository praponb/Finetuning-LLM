from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 14800, "high").when(F.col("c.credit_limit") >= 3500, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.is_active"),
    )
    .agg(F.min(F.col("o.order_date")).alias("first_order_date"))
    .orderBy("first_order_date", "credit_limit_bucket")
)
