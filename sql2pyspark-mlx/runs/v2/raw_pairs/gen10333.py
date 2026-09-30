from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country").isin(["SG", "DE", "JP"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.hour(F.col("we.event_time")).between(7, 13))
    .select(F.col("we.id"), F.col("we.event_time"), F.col("we.session_id"))
    .orderBy(F.col("id").desc())
)
