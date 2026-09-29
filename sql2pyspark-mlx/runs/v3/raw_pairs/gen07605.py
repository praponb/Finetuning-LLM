from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(157, 263))
    .select("id", "duration_sec", "event_type")
)
