from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("session_id") == 3) & (F.col("event_time") >= "2024-07-01"))
    .select("session_id", "page")
    .orderBy(F.col("page").desc(), F.col("session_id").desc())
)
