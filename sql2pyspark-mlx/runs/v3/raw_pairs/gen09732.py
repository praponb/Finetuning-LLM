from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.col("c.city"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
        F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
    )
)
