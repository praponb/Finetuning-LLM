from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(225, 382))
    .groupBy("session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .filter(F.col("total_duration_sec") > 1300)
    .orderBy("total_duration_sec", F.col("stddev_duration_sec").desc())
)
