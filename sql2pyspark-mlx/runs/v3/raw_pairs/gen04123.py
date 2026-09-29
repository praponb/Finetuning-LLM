from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("o.customer_id"), "outer")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.when(F.col("c.credit_limit") >= 16600, "high").when(F.col("c.credit_limit") >= 3800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.min(F.col("o.order_date")).alias("min_order_date"))
    .orderBy("min_order_date", "credit_limit_bucket")
)
