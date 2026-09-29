from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(
        F.when(F.col("duration_sec") >= 495, "LARGE").otherwise("SMALL").alias("duration_sec_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("event_time")) - F.unix_timestamp(F.min("event_time"))) / 60).alias("span_minutes"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") > 1)
    .orderBy(F.col("event_count").asc_nulls_last())
)
