from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.count(F.when(F.col("s.device") == "mobile", 1)).alias("mobile_count"),
    )
    .orderBy(F.col("signup_year").desc())
)
