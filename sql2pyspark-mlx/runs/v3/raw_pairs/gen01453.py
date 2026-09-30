from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([1, 6]))
    .groupBy("page", "session_id", F.year("event_time").alias("event_year"))
    .agg(
        F.max("duration_sec").alias("max_duration_sec"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy(F.col("search_duration_sec").desc(), "page")
)
