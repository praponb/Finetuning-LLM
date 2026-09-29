from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/home")
    .select("id", "session_id", "event_type")
)
