from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("age") > 51) & F.col("country").isin(["DE", "US", "JP"]) & (F.col("signup_date") > "2023-06-01"))
    .withColumn("email_country_key", F.concat_ws("-", "email", "country"))
)

customers_stats = (
    filtered_customers
    .groupBy("city", F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
)

result = customers_stats.orderBy("signup_month", "city")
