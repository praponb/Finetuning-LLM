from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(~F.col("s.country").isin(["JP", "US"]))
    .groupBy(F.col("s.country"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
    .filter(F.col("web_event_count") >= 3)
    .orderBy("country")
    .limit(5)
)
