from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.monthly_fee") >= 70)
    .groupBy(F.col("c.city"))
    .agg(F.count("*").alias("num_subscriptions"))
    .filter(F.col("num_subscriptions") >= 5)
)
