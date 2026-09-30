from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.sum(F.when(F.col("s.browser") == "Safari", 1).otherwise(0)).alias("safari_count"),
        F.sum(F.when(F.col("s.device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .filter(F.col("safari_count") >= 38)
    .orderBy(F.col("signup_cohort").desc(), F.col("mobile_count").desc())
    .limit(1)
)
