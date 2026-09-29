from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_type").isin(["search", "page_view", "click"]) | F.col("session_id").isin([12, 17, 19, 31]))
    .groupBy("event_type")
    .agg(
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy("event_type")
    .limit(5)
)
