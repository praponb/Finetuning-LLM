from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city").isNull())
    .groupBy(F.month("signup_date").alias("signup_month"))
    .agg(
        F.count("age").alias("age_count"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.min("signup_date").alias("first_signup_date"),
        F.sum("age").alias("total_age"),
    )
    .filter(F.col("total_age") < 330)
    .drop("total_age")
)
