from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("end_date").alias("event_date"))
    .intersect(df_customers.select(F.col("signup_date").alias("event_date")))
)
