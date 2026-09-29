from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-07-29") & (F.col("segment").isin(["SMB", "Consumer", "Corporate"]) | (F.col("country") == "JP")))
    .groupBy("country", "is_active")
    .agg(
        F.count("age").alias("age_count"),
        F.max("signup_date").alias("last_signup_date"),
    )
    .orderBy(F.col("last_signup_date").desc(), "age_count")
)
