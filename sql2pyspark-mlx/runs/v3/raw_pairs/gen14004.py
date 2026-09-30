from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") > 248)
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") < 8)
    .select("session_id", "stddev_duration_sec", "duration_sec_count")
    .orderBy(F.col("stddev_duration_sec").desc())
)
