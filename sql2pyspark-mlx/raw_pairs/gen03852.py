from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["page_view", "purchase"]) & (F.to_date("event_time") >= "2023-04-01"))
    .groupBy("event_type")
    .agg(F.min("event_time").alias("first_event_time"))
)
