from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec").between(276, 431))
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.min("event_time").alias("first_event_time"),
    )
    .filter(F.col("max_duration_sec") < 220)
    .select("event_year", "max_duration_sec", "first_event_time")
)
