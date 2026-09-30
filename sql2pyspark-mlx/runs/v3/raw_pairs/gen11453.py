from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.to_date("event_time").alias("event_day"))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
    .filter(F.col("duration_sec_count") >= 4)
    .drop("duration_sec_count")
    .orderBy(F.col("search_count").desc(), "event_day")
    .limit(3)
)
