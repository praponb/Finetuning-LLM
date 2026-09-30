from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit") >= 7500)
    .groupBy(F.col("c.is_active"))
    .agg(F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"))
    .orderBy(F.col("total_monthly_fee").desc(), "is_active")
    .limit(1)
)
