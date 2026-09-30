from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select("id", "page", "event_type", "session_id")
    .orderBy(F.col("page").desc())
)
