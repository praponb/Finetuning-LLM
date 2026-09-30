from pyspark.sql import functions as F

recent_web_events = (
    df_web_events
    .filter((F.col("duration_sec") == 216) & F.hour("event_time").between(10, 15) & F.col("page").isNotNull())
)

summary = (
    recent_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count("duration_sec").alias("duration_sec_count"),
    )
)

result = (
    summary
    .filter(F.col("duration_sec_count") > 6)
    .orderBy(F.col("duration_sec_count").desc(), "event_hour")
    .limit(10)
)
