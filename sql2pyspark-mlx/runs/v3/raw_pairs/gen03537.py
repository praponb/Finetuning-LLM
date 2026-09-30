from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "page")
    .agg((F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"))
    .orderBy("page")
)
