from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "search")
    .groupBy("page")
    .pivot("event_type", ["click", "page_view", "search", "add_to_cart"])
    .agg(F.count("duration_sec"))
    .orderBy("page")
)
