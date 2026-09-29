from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("page") != "/home") & (F.col("event_type") != "purchase"))
    .select("id", "event_time")
)
