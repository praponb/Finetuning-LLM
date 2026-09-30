from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("event_time") >= "2024-12-19") | (F.col("duration_sec") < 130))
)

session_id_summary = (
    filtered_web_events
    .groupBy("session_id", "page")
    .agg(F.min("duration_sec").alias("min_duration_sec"))
)

result = (
    session_id_summary
    .select("session_id", "page", "min_duration_sec")
    .orderBy("min_duration_sec")
)
