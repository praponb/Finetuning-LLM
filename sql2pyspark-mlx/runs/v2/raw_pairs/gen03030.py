from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(F.col("credit_limit").between(6100, 10700) & F.col("is_active"))
    .withColumn("email_filled", F.coalesce("email", F.lit("N/A")))
)

customers_stats = (
    filtered_customers
    .groupBy("country")
    .agg(
        F.round(F.stddev("age"), 2).alias("stddev_age"),
        F.max("signup_date").alias("latest_signup_date"),
    )
)

result = (
    customers_stats
    .filter(F.col("stddev_age") < 61)
    .select("country", "latest_signup_date")
    .orderBy(F.col("country").desc(), "latest_signup_date")
    .limit(10)
)
