from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([18, 39]) & (F.col("duration_sec") >= 279))
    .select("id", "duration_sec", "session_id", "page")
)
