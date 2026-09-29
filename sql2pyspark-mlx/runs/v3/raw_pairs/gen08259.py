from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 366)
    .groupBy("page", F.year("event_time").alias("event_year"))
    .agg(F.min("duration_sec").alias("min_duration_sec"))
    .filter(F.col("min_duration_sec") < 180)
    .select("page", "event_year", "min_duration_sec")
    .orderBy(F.col("event_year").desc())
)
