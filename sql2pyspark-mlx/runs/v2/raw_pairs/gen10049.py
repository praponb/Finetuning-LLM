from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("browser") != "Firefox").alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page").isin(["/home", "/search"]))
    .select(F.col("we.id"), F.col("we.session_id"))
    .orderBy("id", F.col("session_id").desc())
    .limit(3)
)
