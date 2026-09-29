from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([9, 28, 40]))
    .groupBy("page")
    .pivot("event_type", ["search", "page_view", "click", "add_to_cart"])
    .agg(F.max("duration_sec"))
    .orderBy("page")
)
