from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.quarter("event_time").alias("event_quarter"),
        F.date_format("event_time", "yyyy-MM").alias("event_month"),
    )
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
)
