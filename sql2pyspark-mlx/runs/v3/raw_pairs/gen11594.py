from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("name").like("%Kan%") & F.col("age").between(42, 61))
    .groupBy(
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("pct_chiang_mai") < 55)
)
