from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.name").like("O%"))
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.col("c.country"),
    )
    .agg(
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
)
