from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("duration_sec").between(189, 322) & (F.to_date("event_time") >= "2024-12-01"))
)

summary = (
    valid_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("min_duration_sec") >= 200)
    .orderBy(F.col("event_year").desc())
)
