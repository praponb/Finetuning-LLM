from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("event_time") >= "2023-08-01")
    .groupBy("page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") >= 2200)
    .select("page", "total_duration_sec")
    .orderBy("page")
)
