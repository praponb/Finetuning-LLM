from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .select("id", "event_type", "event_time", "duration_sec")
)
