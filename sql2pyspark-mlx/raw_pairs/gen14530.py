from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.min("duration_sec").alias("min_duration_sec"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
    .orderBy("total_duration_sec")
)
