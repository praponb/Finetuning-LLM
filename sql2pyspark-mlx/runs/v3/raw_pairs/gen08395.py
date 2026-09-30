from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.city"))
    .agg(
        (F.max(F.col("we.duration_sec")) - F.min(F.col("we.duration_sec"))).alias("duration_sec_range"),
        F.round(((100 * F.sum(F.when(F.col("we.page") == "/search", 1).otherwise(0))) / F.count("*")), 1).alias("pct_search"),
    )
)
