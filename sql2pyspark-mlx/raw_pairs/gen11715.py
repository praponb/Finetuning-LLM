from pyspark.sql import functions as F

result = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.sum(F.when(F.col("page") == "/search", 1).otherwise(0)).alias("search_count"),
    )
)
