from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("page")
    .agg(
        F.sum(F.when(F.col("event_type") == "search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.sum("duration_sec").alias("total_duration_sec"),
    )
    .orderBy(F.col("total_duration_sec").desc(), "page")
    .limit(20)
)
