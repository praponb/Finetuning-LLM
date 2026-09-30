from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.device").isin(["mobile", "desktop"]))
    .groupBy(F.col("s.browser"))
    .agg(F.count("*").alias("num_web_events"))
    .orderBy("browser", F.col("num_web_events").desc())
)
