from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.age").isNull())
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.count("*").alias("num_subscriptions"))
    .filter(F.col("num_subscriptions") > 1)
    .orderBy("num_subscriptions", F.col("signup_month").desc())
)
