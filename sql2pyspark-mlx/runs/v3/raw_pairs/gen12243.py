from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.month("event_time").alias("event_month"), "event_type")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("event_type") == "click", 1).otherwise(0)).alias("click_count"),
    )
    .orderBy(F.col("total_duration_sec").desc(), "event_month")
)
