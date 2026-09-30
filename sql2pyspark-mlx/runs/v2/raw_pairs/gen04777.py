from pyspark.sql import functions as F

valid_customers = (
    df_customers
    .filter(F.col("name").like("%FRANK%"))
    .withColumn("email_normalized", F.upper(F.trim("email")))
)

customers_stats = (
    valid_customers
    .groupBy("segment")
    .agg(F.max("signup_date").alias("last_signup_date"))
)

result = customers_stats.orderBy("last_signup_date")
