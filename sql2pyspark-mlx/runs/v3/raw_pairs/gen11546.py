from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .union(df_customers.select("signup_date", F.lit("customer").alias("source")))
    .distinct()
)
