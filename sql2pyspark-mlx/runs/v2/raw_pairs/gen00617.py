from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("age") >= 53) & (F.col("segment") == "Consumer"))
    .withColumn("name_prefix", F.substring("name", 1, 2))
)

signup_month_summary = (
    filtered_customers
    .groupBy(F.date_format("signup_date", "yyyy-MM").alias("signup_month"))
    .agg(
        F.max("age").alias("max_age"),
        F.count("*").alias("total_count"),
    )
)

result = (
    signup_month_summary
    .orderBy(F.col("signup_month").desc())
    .limit(3)
)
