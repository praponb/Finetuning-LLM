from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .pivot("event_type", ["click", "search", "page_view", "purchase"])
    .agg(F.count("duration_sec"))
    .orderBy("page")
)
