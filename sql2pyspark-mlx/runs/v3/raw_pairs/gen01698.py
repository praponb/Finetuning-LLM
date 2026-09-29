from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
    .filter(F.col("stddev_duration_sec") < 35)
    .orderBy(F.col("page").desc())
    .limit(50)
)
