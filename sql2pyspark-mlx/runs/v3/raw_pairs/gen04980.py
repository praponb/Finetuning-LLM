from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id", "page")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy(F.col("session_id").desc(), F.col("max_duration_sec").desc())
)
