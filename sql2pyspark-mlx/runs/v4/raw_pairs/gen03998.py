from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.col("s.country") == "UK")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(F.max(F.col("we.duration_sec")).alias("max_duration_sec"))
)
