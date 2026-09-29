from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("c.city") == "Singapore")
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/checkout", F.col("we.duration_sec")).otherwise(0)).alias("checkout_duration_sec"),
    )
    .filter(F.col("checkout_duration_sec") >= 42)
)
