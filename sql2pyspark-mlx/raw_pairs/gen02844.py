from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(43, 66))
    .groupBy("is_active", "city")
    .agg(F.max("signup_date").alias("latest_signup_date"))
    .orderBy("city")
)
