from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") == "page_view")
    .select("event_time", "page")
    .orderBy("page")
)
