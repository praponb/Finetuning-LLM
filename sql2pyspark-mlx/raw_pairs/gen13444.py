from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .groupBy("event_type", "page")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .filter(F.col("max_duration_sec") < 380)
)
