from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 11, 14, 21]))
    .select("duration_sec")
)
