from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 525, "LARGE").otherwise("SMALL"))
    .cube("duration_sec_tier")
    .agg(
        F.max("event_time").alias("ended_at"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_rows"),
        F.max("event_time").alias("ended_at_2"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("search_rows") < 1000)
)
