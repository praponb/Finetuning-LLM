from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([21, 23, 29]))
    .agg(F.avg("duration_sec").alias("avg_duration_sec"))
)
