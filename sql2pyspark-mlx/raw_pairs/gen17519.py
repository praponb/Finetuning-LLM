from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.filter(F.col("session_id").isin([23, 40])).alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.country").isin(["US", "TH", "JP"]))
    .select(F.col("s.id"), F.col("s.device"), F.col("s.country"), F.col("s.browser"))
    .orderBy(F.col("device").desc())
)
