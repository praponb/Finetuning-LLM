from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date")
    .subtract(df_customers.select(F.col("signup_date").alias("start_date")))
)
