from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.hour("event_time").between(11, 15) & (F.col("page") == "/checkout"))
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.count("*").alias("web_event_count"),
    )
)
