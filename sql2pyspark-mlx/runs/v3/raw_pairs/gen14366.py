from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(12, 15) & F.col("session_id").isin([2, 6]))
)
