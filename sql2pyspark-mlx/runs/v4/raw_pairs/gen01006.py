from pyspark.sql import functions as F

filtered_customers = df_customers.filter((F.col("city") == "Berlin") & F.col("name").isNotNull())

city_summary = (
    filtered_customers
    .groupBy("city", "is_active")
    .agg(F.max("signup_date").alias("latest_signup_date"))
)

result = city_summary.orderBy(F.col("city").desc())
