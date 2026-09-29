from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.year("signup_date") < 2024, "before_2024").when(F.year("signup_date") == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        "segment",
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
        F.count("*").alias("customer_count"),
    )
    .filter(F.col("customer_count") >= 8)
    .select("signup_cohort", "segment", "pct_new_york", "customer_count")
    .orderBy(F.col("customer_count").desc())
)
