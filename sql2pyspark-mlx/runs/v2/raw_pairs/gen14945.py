from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~((F.col("event_type") == "page_view") & ((F.col("event_type") == "add_to_cart") | (F.col("duration_sec") < 233))))
    .select("id", "duration_sec", "event_type", "event_time")
)
