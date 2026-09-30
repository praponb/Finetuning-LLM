from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("page").isNotNull())
    .groupBy("session_id")
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
)
