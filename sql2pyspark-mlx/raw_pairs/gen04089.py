from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date").between("2023-05-01", "2024-10-01") & (F.col("name").like("bo%") | F.col("name").isNull()))
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count("credit_limit").alias("credit_limit_count"),
        F.avg("age").alias("avg_age"),
    )
    .filter(F.col("avg_age") > 61)
)
