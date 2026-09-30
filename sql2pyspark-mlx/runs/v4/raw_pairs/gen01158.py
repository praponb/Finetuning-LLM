from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.device").isin(["desktop", "mobile"]))
    .select(F.col("s.id"), F.col("s.browser"), F.col("s.country"), F.col("s.device"))
    .orderBy(F.col("id").desc())
)
