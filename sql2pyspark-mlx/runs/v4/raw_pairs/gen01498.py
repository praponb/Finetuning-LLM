from pyspark.sql import functions as F

result = (
    df_customers
    .select(F.col("signup_date").alias("event_date"))
    .subtract(
        df_subscriptions
        .filter(F.col("start_date") < "2023-05-01")
        .select(F.col("start_date").alias("event_date"))
    )
)
