from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter((F.col("duration_sec") > 201) | (F.col("event_time") >= "2023-08-10"))
    .withColumn("clean_page", F.trim("page"))
)

agg = (
    filtered_web_events
    .groupBy("event_type")
    .agg(F.count("*").alias("web_event_count"))
)

result = (
    agg
    .select("event_type", "web_event_count")
    .orderBy("event_type")
    .limit(5)
)
