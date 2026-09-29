from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(~F.col("status").isin(["PAUSED", "ACTIVE", "EXPIRED"]))
    .select("end_date")
    .unionByName(df_customers.select(F.col("signup_date").alias("end_date")))
)
