from pyspark.sql import functions as F

filtered_web_events = (
    df_web_events
    .filter(~(F.col("session_id").isin([18, 23, 28]) | F.col("duration_sec").between(190, 373)))
)

summary = (
    filtered_web_events
    .groupBy(
        F.hour("event_time").alias("event_hour"),
        F.year("event_time").alias("event_year"),
    )
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    summary
    .select("event_hour", "total_duration_sec")
    .orderBy(F.col("event_hour").desc(), "total_duration_sec")
)
