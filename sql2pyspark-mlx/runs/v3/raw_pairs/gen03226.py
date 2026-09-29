from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("signup_date") >= "2025-10-01") & F.col("city").isin(["Tokyo", "Bangkok"]))
    .groupBy(
        F.month("signup_date").alias("signup_month"),
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(F.count(F.when(F.col("country") == "UK", 1)).alias("uk_count"))
    .orderBy(F.col("signup_month").desc(), F.col("signup_cohort").desc())
    .limit(5)
)
