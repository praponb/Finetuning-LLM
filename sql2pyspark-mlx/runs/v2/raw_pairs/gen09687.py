from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.max("event_time").alias("last_event_time"),
    )
    .orderBy("last_event_time", "session_id")
)
