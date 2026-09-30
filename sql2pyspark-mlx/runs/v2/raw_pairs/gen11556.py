from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.to_date("event_time") >= "2024-07-24") & (F.col("page") == "/cart") & (F.col("page") == "/home"))
    .withColumn("event_type_page", F.concat("event_type", F.lit(", "), "page"))
)

web_events_stats = (
    recent_web_events
    .groupBy("page")
    .agg(F.count("duration_sec").alias("duration_sec_count"))
)

result = (
    web_events_stats
    .filter(F.col("duration_sec_count") >= 5)
    .select("page", "duration_sec_count")
    .orderBy(F.col("duration_sec_count").desc())
)
