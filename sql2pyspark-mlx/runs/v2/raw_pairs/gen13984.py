from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("s.customer_id").isin([23, 36]))
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy(F.col("id").desc())
)
