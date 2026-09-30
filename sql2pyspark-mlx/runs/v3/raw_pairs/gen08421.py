from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([4, 17, 29]))
    .groupBy("page")
    .pivot("event_type", ["click", "page_view", "add_to_cart"])
    .agg(F.max("duration_sec"))
    .orderBy("page")
)
