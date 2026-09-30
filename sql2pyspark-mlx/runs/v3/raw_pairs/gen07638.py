from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.year("signup_date") >= 2024)
    .groupBy("is_active")
    .agg(
        F.sum("age").alias("total_age"),
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sum(F.when(F.col("country") == "UK", 1).otherwise(0)).alias("uk_count"),
    )
    .filter(F.col("uk_count") < 22)
    .drop("uk_count")
    .orderBy("total_age", F.col("total_credit_limit").desc(), "is_active")
    .limit(1)
)
