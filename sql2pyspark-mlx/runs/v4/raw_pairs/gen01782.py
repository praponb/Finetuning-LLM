from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.date_trunc("MONTH", F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.count(F.when(F.col("we.event_type") == "search", 1)).alias("search_count"))
    .orderBy("signup_month")
)
