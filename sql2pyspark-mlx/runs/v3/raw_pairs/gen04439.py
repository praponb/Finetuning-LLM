from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.to_date("event_time") >= "2025-09-01") | F.hour("event_time").between(11, 18))
    .groupBy("page")
    .agg(
        F.min("event_time").alias("first_event_time"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("row_count") < 4)
    .orderBy("first_event_time")
)
