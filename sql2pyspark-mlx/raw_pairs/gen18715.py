from pyspark.sql import functions as F

filtered_customers = (
    df_customers
    .filter((F.col("signup_date") > "2025-09-01") | (F.col("signup_date") <= "2023-05-05"))
)

summary = (
    filtered_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 43, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.count(F.when(F.col("segment") == "Corporate", 1)).alias("corporate_count"))
)

result = (
    summary
    .filter(F.col("corporate_count") < 6)
    .orderBy(F.col("corporate_count").desc(), F.col("age_bucket").desc())
)
