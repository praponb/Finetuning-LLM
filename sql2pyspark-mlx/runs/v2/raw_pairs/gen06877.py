from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & F.col("email").like("%@gmail.com"))
    .groupBy(F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("credit_limit").alias("max_credit_limit"),
        F.sum(F.when(F.col("city") == "Tokyo", F.col("age")).otherwise(0)).alias("tokyo_age"),
    )
    .filter(F.col("tokyo_age") < 44)
    .drop("tokyo_age")
)
