from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("we.session_id").isin([20, 25, 30]))
    .select(F.col("s.country"), F.col("s.browser"), F.col("we.page"))
)
