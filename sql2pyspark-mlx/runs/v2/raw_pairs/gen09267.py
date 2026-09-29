from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"))
    .agg(F.count(F.col("we.duration_sec")).alias("duration_sec_count"))
    .filter(F.col("duration_sec_count") >= 5)
    .orderBy(F.col("duration_sec_count").desc())
)
