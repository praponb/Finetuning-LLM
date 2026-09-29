from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") <= 31)
    .groupBy(F.date_trunc("MONTH", "signup_date").alias("signup_month"), "segment")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy(F.col("segment").desc(), F.col("last_signup_date").desc())
)
