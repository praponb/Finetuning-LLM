from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("duration_sec") <= 268)
    .groupBy(F.date_trunc("MONTH", "event_time").alias("event_month"), "page")
    .agg(
        F.count("*").alias("num_web_events"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") > 320)
    .drop("max_duration_sec")
)
