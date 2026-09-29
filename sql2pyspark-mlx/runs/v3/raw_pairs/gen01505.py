from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.to_date("event_time") < "2024-12-01")
    .select("page", "event_type")
    .distinct()
    .orderBy(F.col("event_type").desc())
)
