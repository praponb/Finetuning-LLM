from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.browser").isin(["Safari", "Edge"]))
    .groupBy(F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"))
    .agg(
        F.count(F.when(F.col("we.event_type") == "click", 1)).alias("click_count"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
)
