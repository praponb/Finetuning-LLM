from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(F.month(F.col("s.started_at")).alias("started_month"))
    .agg(F.sum(F.col("we.duration_sec")).alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") > 1300)
    .orderBy(F.col("total_duration_sec").desc())
)
