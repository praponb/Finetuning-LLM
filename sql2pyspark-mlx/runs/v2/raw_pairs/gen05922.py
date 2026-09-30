from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("s.customer_id").isNull())
    .groupBy(F.col("s.device"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy(F.col("device").desc())
)
