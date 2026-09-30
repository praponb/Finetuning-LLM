from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") == 237)
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)
