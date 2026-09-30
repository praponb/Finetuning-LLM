from pyspark.sql import functions as F

result = (
    df_web_events
    .withColumn("duration_sec_tier", F.when(F.col("duration_sec") >= 439, "LARGE").otherwise("SMALL"))
    .withColumn("week_start", F.date_trunc("WEEK", "event_time"))
    .rollup("duration_sec_tier", "week_start")
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.size(F.collect_set("page")).alias("num_pages"),
        F.count(F.when(F.col("page") == "/checkout", 1)).alias("checkout_count"),
        F.count_if(F.col("page") == "/search").alias("search_events"),
        F.count("*").alias("cnt"),
        F.grouping("duration_sec_tier").alias("gid"),
    )
    .filter(F.col("checkout_count") >= 1)
)
