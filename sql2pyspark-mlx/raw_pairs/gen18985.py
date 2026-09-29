from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("end_date")
    .unionByName(df_customers.select(F.col("signup_date").alias("end_date")))
    .distinct()
    .orderBy(F.col("end_date").desc())
)
