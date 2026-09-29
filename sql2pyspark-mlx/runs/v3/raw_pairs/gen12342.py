from pyspark.sql import functions as F

result = (
    df_web_events
    .filter((F.col("event_type").isin(["page_view", "click", "search"]) & (F.col("session_id") == 36)) | F.col("session_id").isin([6, 11, 23]))
    .agg(
        F.sum(F.when(F.col("event_type") == "search", 1).otherwise(0)).alias("search_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
    )
)
