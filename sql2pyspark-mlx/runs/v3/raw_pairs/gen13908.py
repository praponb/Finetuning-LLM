from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 211) & (F.col("event_type") == "search"))
    .select("event_time", F.substring("event_type", 1, 4).alias("event_type_prefix"))
)
