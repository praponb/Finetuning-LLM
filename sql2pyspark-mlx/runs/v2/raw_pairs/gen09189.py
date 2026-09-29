from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.hour("event_time").between(10, 14)).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(F.col("s.browser") != "Firefox")
    .select(F.col("s.id"), F.col("s.device"))
)
