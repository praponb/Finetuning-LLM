from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter(~F.col("city").isin(["Bangkok", "New York", "Chiang Mai"]) & (F.col("signup_date") > "2024-03-01"))
    .withColumn("country_email", F.concat("country", F.lit(", "), "email"))
)

segment_summary = (
    filtered_customers
    .groupBy("segment", F.month("signup_date").alias("signup_month"))
    .agg((F.max("age") - F.min("age")).alias("age_range"))
)

result = (
    segment_summary
    .select("segment", "signup_month", "age_range")
    .orderBy("segment", "signup_month")
    .limit(20)
)
