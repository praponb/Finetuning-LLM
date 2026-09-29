from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([21, 22, 25, 37]) & (F.col("duration_sec") < 469))
    .select("id", "session_id")
)
