from pyspark.sql import functions as F

filtered_customers = df_customers.filter(F.col("credit_limit") > 15100)

segment_summary = (
    filtered_customers
    .groupBy("segment", F.year("signup_date").alias("signup_year"))
    .agg(
        F.max("age").alias("max_age"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
)

result = (
    segment_summary
    .filter(F.col("bangkok_count") < 73)
    .select("segment", "signup_year", "max_age", "bangkok_count")
    .orderBy("signup_year", "segment")
    .limit(5)
)
