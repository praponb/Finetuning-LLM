from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower("email").like("%vy@%"))
    .select(F.col("signup_date").alias("event_date"))
    .subtract(df_subscriptions.select(F.col("start_date").alias("event_date")))
)
