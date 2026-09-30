from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([13, 16, 37]))
    .groupBy(
        F.when(F.year("event_time") < 2024, "before_2024").when(F.year("event_time") == 2024, "2024").otherwise("after_2024").alias("event_cohort"),
        "event_type",
    )
    .agg(
        F.round(F.avg("duration_sec"), 2).alias("avg_duration_sec"),
        F.count("duration_sec").alias("duration_sec_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
    .filter(F.col("avg_duration_sec") > 69)
    .orderBy("avg_duration_sec")
)
