from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_time") < "2023-03-21") & (F.col("duration_sec") > 380))
    .select("session_id", "duration_sec")
    .orderBy(F.col("duration_sec").desc(), "session_id")
)
