from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.session_id").isin([8, 39]))
    .select(
        F.col("we.event_time"),
        F.col("we.page"),
        F.col("s.device"),
        F.substring(F.col("s.country"), 1, 4).alias("country_prefix"),
    )
    .orderBy("page")
)
