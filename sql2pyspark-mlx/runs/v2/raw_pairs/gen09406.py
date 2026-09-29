from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["TH", "SG", "JP"]))
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .orderBy("browser", "id")
    .limit(20)
)
