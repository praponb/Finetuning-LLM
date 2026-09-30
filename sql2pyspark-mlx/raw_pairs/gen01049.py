from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([30, 33, 34]) & (F.col("page") == "/cart"))
    .groupBy("page", F.year("event_time").alias("event_year"))
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"))
)
