from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("duration_sec") >= 173) & (F.col("page") == "/home"))
    .groupBy("event_type")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
    .filter(F.col("max_duration_sec") >= 360)
    .drop("max_duration_sec")
)
