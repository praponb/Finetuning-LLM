from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") != "/home") & F.col("session_id").isin([13, 20]))
    .select("event_time", "duration_sec")
    .orderBy("event_time")
)
