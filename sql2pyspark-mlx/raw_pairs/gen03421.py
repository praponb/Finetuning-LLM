from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.device"))
    .agg(F.count(F.when(F.col("we.page") == "/home", 1)).alias("home_count"))
    .orderBy(F.col("home_count").desc())
)
