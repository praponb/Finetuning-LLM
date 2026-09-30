from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.customer_id").isNull())
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.min(F.col("we.duration_sec")).alias("min_duration_sec"),
    )
    .filter(F.col("min_duration_sec") >= 260)
    .orderBy("browser")
)
