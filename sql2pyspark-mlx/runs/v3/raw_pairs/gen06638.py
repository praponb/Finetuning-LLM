from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("event_type").isin(["search", "click"]) & (F.col("page") == "/checkout"))
    .groupBy("page", "event_type")
    .agg(F.max("duration_sec").alias("max_duration_sec"))
    .orderBy("max_duration_sec")
)
