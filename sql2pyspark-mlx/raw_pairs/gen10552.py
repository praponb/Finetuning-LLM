from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.min("signup_date").alias("min_signup_date"),
        F.count("*").alias("total_count"),
    )
    .filter(F.col("total_count") >= 7)
    .orderBy("signup_month")
)
