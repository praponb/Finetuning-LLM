from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .groupBy("is_active")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Singapore", 1).otherwise(0))) / F.count("*")), 1).alias("pct_singapore"),
        F.min("signup_date").alias("min_signup_date"),
    )
    .filter(F.col("pct_singapore") < 33)
    .orderBy("min_signup_date")
)
