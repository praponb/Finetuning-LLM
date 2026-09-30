from pyspark.sql import functions as F

result = (
    df_web_events
    .select("id", "page", "session_id", "duration_sec")
    .orderBy(F.col("session_id").desc())
)
