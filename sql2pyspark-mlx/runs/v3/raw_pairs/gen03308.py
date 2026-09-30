from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(8, 14) & F.col("duration_sec").between(345, 454))
)
