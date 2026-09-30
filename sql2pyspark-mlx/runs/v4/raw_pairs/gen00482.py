from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") <= 16100) & F.lower("email").like("%iam%") & (F.col("city") == "Chiang Mai"))
    .groupBy("is_active", "city")
    .agg(
        F.max("signup_date").alias("latest_signup_date"),
        F.round(F.stddev("credit_limit"), 2).alias("stddev_credit_limit"),
    )
)
