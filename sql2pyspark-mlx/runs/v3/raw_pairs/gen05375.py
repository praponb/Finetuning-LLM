from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page") == "/search")
    .select("event_time", "page")
    .orderBy("event_time")
)
