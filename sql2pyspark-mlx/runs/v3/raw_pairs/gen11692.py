from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("city")
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy("latest_signup_date", "city")
    .limit(1)
)
