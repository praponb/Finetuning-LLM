from pyspark.sql import functions as F

result = (
    df_web_events
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.avg("duration_sec").alias("avg_duration_sec"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
    .orderBy(F.col("avg_duration_sec").desc())
)
