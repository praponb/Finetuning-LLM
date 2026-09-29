from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isNull())
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .filter(F.col("row_count") < 3)
    .select("signup_month", "row_count", "last_signup_date")
)
