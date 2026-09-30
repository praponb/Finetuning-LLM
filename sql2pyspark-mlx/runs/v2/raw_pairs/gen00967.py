from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 2)
    .groupBy("page")
    .pivot("event_type", ["purchase", "page_view"])
    .agg(F.max("duration_sec"))
    .orderBy("page")
)
