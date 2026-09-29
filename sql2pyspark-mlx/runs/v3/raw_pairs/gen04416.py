from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(((F.col("page") == "/home") & F.col("session_id").isin([25, 26, 32])) | (F.col("event_type") == "add_to_cart"))
    .select("duration_sec", "page", "event_type")
    .orderBy("event_type")
)
