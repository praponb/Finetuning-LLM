from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.max("signup_date").alias("latest_signup_date"),
    )
    .filter(F.col("de_count") > 8)
    .orderBy(F.col("latest_signup_date").desc())
)
