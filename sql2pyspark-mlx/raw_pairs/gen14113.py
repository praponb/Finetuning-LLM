from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 36) & F.col("event_type").isin(["click", "search", "purchase"]))
    .groupBy(F.quarter("event_time").alias("event_quarter"))
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
    .orderBy(F.col("event_quarter").desc())
)
