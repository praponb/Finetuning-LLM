from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("event_type", "page")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
    .filter(F.col("total_duration_sec") >= 2600)
    .orderBy(F.col("page").desc(), F.col("total_duration_sec").desc())
)
