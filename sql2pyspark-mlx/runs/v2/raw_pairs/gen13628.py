from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.country") == "DE")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.max(F.col("we.duration_sec")).alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") >= 450)
    .orderBy("signup_year")
)
