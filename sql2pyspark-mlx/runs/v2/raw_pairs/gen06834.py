from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "outer")
    .select(F.col("we.session_id"), F.col("s.country"))
    .orderBy("country")
)
