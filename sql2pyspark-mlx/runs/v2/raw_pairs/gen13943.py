from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.countDistinct("duration_sec").alias("unique_sessions"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
    .orderBy(F.col("unique_sessions").desc())
)
