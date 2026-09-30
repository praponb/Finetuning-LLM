from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2025-04-01")
    .select("event_type", "page")
    .distinct()
)
