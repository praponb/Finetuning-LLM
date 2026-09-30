from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.date_format(F.col("s.started_at"), "yyyy-MM").alias("started_month"),
        F.col("s.browser"),
    )
    .agg(F.round(F.avg(F.col("we.duration_sec")), 2).alias("avg_duration_sec"))
    .orderBy(F.col("avg_duration_sec").desc())
)
