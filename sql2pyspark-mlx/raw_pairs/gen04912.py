from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.is_active"))
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.when(F.col("c.credit_limit") >= 15100, "high").when(F.col("c.credit_limit") >= 7300, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.min(F.col("o.amount")).alias("min_amount"))
)
