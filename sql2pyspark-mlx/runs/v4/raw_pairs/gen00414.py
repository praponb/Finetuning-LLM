from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") >= 421)
    .select("id", "duration_sec", F.upper("event_type").alias("event_type_upper"))
)
