from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .pivot("event_type", ["add_to_cart", "search"])
    .agg(F.avg("duration_sec"))
)
