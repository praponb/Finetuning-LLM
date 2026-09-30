from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter((F.col("session_id") == 33) & (F.col("event_time") >= "2025-10-18"))
)

page_summary = (
    valid_web_events
    .groupBy("page", "event_type")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
    )
)

result = (
    page_summary
    .select("page", "event_type", "duration_sec_count", "page_values")
    .orderBy("event_type", "page")
    .limit(5)
)
