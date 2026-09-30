from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country") == "SG").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_anti")
    .filter(F.col("we.session_id").isin([9, 21, 27]))
    .select(F.col("we.id"), F.col("we.page"), F.col("we.event_time"))
)
