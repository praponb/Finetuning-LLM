from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("device") == "mobile").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.session_id").isin([10, 14, 35]))
    .select(F.col("we.id"), F.col("we.page"))
    .orderBy("page", F.col("id").desc())
)
