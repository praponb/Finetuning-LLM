from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter((F.col("s.country") == "UK") | F.col("we.session_id").isin([8, 16, 20, 28]))
    .select(
        F.col("s.started_at"),
        F.col("s.browser"),
        F.col("we.event_time"),
        F.when(F.col("we.duration_sec") >= 538, "High").when(F.col("we.duration_sec") >= 104, "Medium").otherwise("Low").alias("duration_sec_level"),
    )
    .orderBy(F.col("browser").desc())
)
