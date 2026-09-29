from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") == "/product") & (F.col("session_id") == 5))
    .orderBy(F.col("event_type").desc())
)
