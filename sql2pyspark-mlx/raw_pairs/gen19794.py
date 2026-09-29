from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"), F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.count("*").alias("num_web_events"),
        F.count(F.when(F.col("we.page") == "/checkout", 1)).alias("checkout_count"),
    )
)
