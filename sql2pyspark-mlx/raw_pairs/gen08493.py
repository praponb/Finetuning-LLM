from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("s.browser"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/checkout", 1).otherwise(0)).alias("checkout_count"),
        F.avg(F.col("we.duration_sec")).alias("avg_duration_sec"),
    )
    .orderBy("checkout_count")
)
