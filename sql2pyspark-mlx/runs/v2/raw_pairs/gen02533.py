from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy(F.hour("event_time").alias("event_hour"))
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        F.count(F.when(F.col("event_type") == "search", 1)).alias("search_count"),
    )
    .filter(F.col("search_count") >= 3)
    .select("event_hour", "page_list", "search_count")
)
