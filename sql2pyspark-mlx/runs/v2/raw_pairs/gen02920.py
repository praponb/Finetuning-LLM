from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy("country")
    .limit(10)
)
