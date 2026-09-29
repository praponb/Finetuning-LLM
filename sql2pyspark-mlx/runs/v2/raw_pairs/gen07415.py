from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.country"))
    .agg(F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"))
    .filter(F.col("total_monthly_fee") >= 1360)
)
