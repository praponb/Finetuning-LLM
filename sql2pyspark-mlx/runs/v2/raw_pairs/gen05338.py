from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
    )
    .filter(F.col("de_count") > 9)
    .select("signup_month", "de_count", "pct_corporate")
    .orderBy("de_count", "signup_month")
)
