from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("kind"))
    .union(df_customers.select("signup_date", F.lit("customer").alias("kind")))
    .distinct()
    .orderBy(F.col("kind").desc())
)
