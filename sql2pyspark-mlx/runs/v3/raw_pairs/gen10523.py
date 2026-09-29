from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "page")
    .agg(F.countDistinct("duration_sec").alias("unique_event_types"))
    .filter(F.col("unique_event_types") > 4)
    .select("session_id", "page", "unique_event_types")
)
