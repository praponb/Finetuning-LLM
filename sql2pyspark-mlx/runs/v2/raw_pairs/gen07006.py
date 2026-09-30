from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id").isin([5, 10, 25, 30]))
    .groupBy("event_type", "session_id")
    .agg(
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.min("event_time").alias("first_event_time"),
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("page") == "/product", F.col("duration_sec")).otherwise(0)).alias("product_duration_sec"),
    )
    .filter(F.col("product_duration_sec") > 54)
    .drop("product_duration_sec")
)
