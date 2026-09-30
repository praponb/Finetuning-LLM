from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["page_view", "search"])
    .agg(F.sum("duration_sec"))
    .orderBy("page")
)
