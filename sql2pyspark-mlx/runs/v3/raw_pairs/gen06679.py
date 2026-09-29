from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["search", "add_to_cart", "page_view"])
    .agg(F.max("duration_sec"))
    .orderBy("page")
)
