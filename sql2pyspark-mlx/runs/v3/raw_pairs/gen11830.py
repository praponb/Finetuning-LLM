from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "Berlin")
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .filter(F.col("bangkok_count") < 63)
    .select("signup_month", "bangkok_count")
)
