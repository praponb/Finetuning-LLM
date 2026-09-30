from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.alias("s"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.month(F.col("c.signup_date")).isin([1, 4, 5]))
    .groupBy(F.col("c.id"))
    .agg(F.count(F.col("s.id")).alias("subscription_count"))
    .filter(F.col("subscription_count") > 5)
    .orderBy(F.col("subscription_count").desc(), F.col("id").desc())
    .limit(50)
)
