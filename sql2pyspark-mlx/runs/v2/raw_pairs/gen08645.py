from pyspark.sql import functions as F

max_duration_sec = df_web_events.filter(F.col("session_id").isin([23, 28])).agg(F.max("duration_sec")).first()[0]

result = (
    df_web_events
    .filter(F.col("duration_sec") >= max_duration_sec)
    .select("id", "duration_sec", "event_time", "event_type")
    .orderBy(F.col("id").desc())
    .limit(10)
)
