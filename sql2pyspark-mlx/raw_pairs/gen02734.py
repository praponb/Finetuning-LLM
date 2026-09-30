from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.col("c.is_active"))
    .agg(F.countDistinct(F.col("s.monthly_fee")).alias("unique_customers"))
)
