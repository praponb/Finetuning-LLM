from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"))
    .agg(
        F.max(F.col("we.duration_sec")).alias("max_duration_sec"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc(), F.col("signup_month").desc())
    .limit(50)
)
