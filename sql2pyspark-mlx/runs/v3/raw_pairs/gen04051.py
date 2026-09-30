from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("is_active") | (F.col("age") < 44))
    .withColumn("email_normalized", F.upper(F.trim("email")))
)

customers_stats = (
    recent_customers
    .groupBy(
        F.month("signup_date").alias("signup_month"),
        F.year("signup_date").alias("signup_year"),
    )
    .agg(
        F.count("age").alias("age_count"),
        F.count(F.when(F.col("city") == "New York", 1)).alias("new_york_count"),
    )
)

result = (
    customers_stats
    .filter(F.col("age_count") > 7)
    .orderBy(F.col("new_york_count").desc())
)
