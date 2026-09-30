from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["search", "page_view", "click"])
    .agg(F.count("duration_sec"))
    .orderBy("page")
)
