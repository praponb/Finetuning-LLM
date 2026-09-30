from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 243) & ((F.col("page") == "/home") | (F.to_date("event_time") >= "2023-05-06")))
    .select("event_type", "duration_sec", F.hour("event_time").alias("event_time_hour"))
    .orderBy(F.col("duration_sec").desc())
)
