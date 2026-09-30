from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"))
)
