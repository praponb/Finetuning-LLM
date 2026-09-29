from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([11, 12]))
    .groupBy("page")
    .pivot("event_type", ["search", "add_to_cart", "page_view"])
    .agg(F.sum("duration_sec"))
)
