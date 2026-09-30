from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 16, 37]))
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("*").alias("web_event_count"),
    )
)
