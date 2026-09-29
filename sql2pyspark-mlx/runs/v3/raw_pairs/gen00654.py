from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["purchase", "page_view", "search"]))
    .select("id", "event_type", F.upper("event_type").alias("event_type_upper"))
)
