from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type") != "purchase")
    .select("page", "event_type")
    .distinct()
)
