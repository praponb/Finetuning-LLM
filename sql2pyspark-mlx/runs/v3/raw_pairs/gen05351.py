from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["add_to_cart", "search", "page_view"])
    .agg(F.count("duration_sec"))
)
