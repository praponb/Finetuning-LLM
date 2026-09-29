from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") | F.col("session_id").isin([1, 6, 28]))
    .orderBy("event_type", F.col("duration_sec").desc())
)
