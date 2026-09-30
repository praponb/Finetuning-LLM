from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["search", "page_view", "add_to_cart"])
    .agg(F.sum("duration_sec"))
    .orderBy("page")
)
