from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") == 374) & F.col("session_id").isin([1, 12, 27]))
    .select("id", "event_type", F.ceil("duration_sec").alias("duration_sec_whole"))
)
