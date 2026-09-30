from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "click")
    .groupBy("page")
    .pivot("event_type", ["click", "add_to_cart", "search", "page_view"])
    .agg(F.count("duration_sec"))
)
