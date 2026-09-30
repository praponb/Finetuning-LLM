from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & (F.lower(F.trim("email")) == "alice@gmail.com"))
    .groupBy("country", "is_active", F.year("signup_date").alias("signup_year"))
    .agg(
        F.count("age").alias("age_count"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
    .orderBy(F.col("signup_year").desc(), "country", "is_active")
    .limit(10)
)
