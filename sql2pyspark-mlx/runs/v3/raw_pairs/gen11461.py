from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.is_active"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.sum(F.when(F.col("s.browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
    )
)
