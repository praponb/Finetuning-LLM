from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.year("event_time").alias("event_year"))
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.sum(F.when(F.col("page") == "/search", F.col("duration_sec")).otherwise(0)).alias("search_duration_sec"),
    )
    .orderBy("search_duration_sec")
)
