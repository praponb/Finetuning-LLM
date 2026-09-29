from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(F.col("session_id") == 12)
    .cube("page", "event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("event_type"))).alias("event_type_values"),
        F.count(F.when(F.col("event_type") == "click", 1)).alias("click_count"),
    )
)
