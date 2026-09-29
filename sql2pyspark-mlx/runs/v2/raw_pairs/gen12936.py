from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["page_view", "search"]))
    .groupBy("event_type", F.hour("event_time").alias("event_hour"))
    .agg(F.count("duration_sec").alias("duration_sec_count"))
    .orderBy("event_type")
)
