from pyspark.sql import functions as F

base = df_web_events.filter(F.col("event_time") < "2023-09-17")

summary = (
    base
    .groupBy("session_id")
    .agg(
        F.max("event_time").alias("latest_event_time"),
        F.max("event_time").alias("last_event_time"),
    )
)

result = summary
