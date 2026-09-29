from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.country"))
    .agg(F.count("*").alias("subscription_count"))
    .orderBy("country")
)
