from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select(F.col("start_date").alias("event_date"))
    .unionByName(
        df_customers
        .filter(F.lower(F.trim("email")) == "henry@startup.io")
        .select(F.col("signup_date").alias("event_date"))
    )
)
