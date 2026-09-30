from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
        F.max("event_time").alias("last_event_time"),
    )
    .filter(F.col("duration_sec_range") >= 59)
)
