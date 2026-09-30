from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull() & (F.length("name") > 7))
    .groupBy("is_active")
    .agg(
        F.min("signup_date").alias("first_signup_date"),
        F.sum(F.when(F.col("country") == "US", 1).otherwise(0)).alias("us_count"),
    )
)
