from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.min(F.col("s.started_at")).alias("min_started_at"),
        F.countDistinct(F.col("s.browser")).alias("unique_browsers"),
    )
    .orderBy("signup_year")
)
