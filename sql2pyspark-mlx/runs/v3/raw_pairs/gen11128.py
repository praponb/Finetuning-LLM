from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", F.year("event_time").alias("event_year"))
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .filter(F.col("duration_sec_range") > 66)
    .orderBy(F.col("duration_sec_range").desc())
)
