from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([3, 8, 25, 30]))
    .select("id", "page", "event_type", F.length("page").alias("page_length"))
)
