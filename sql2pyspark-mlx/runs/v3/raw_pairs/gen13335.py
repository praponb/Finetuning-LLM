from pyspark.sql import functions as F

valid_web_events = (
    df_web_events
    .filter(F.col("session_id").isin([22, 24, 28]) & F.col("session_id").isin([27, 30, 38]) & F.col("session_id").isin([6, 9]))
)

summary = (
    valid_web_events
    .groupBy("event_type")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.min("duration_sec").alias("min_duration_sec"),
    )
)

result = (
    summary
    .filter(F.col("min_duration_sec") > 420)
    .select("event_type", "page_values", "min_duration_sec")
    .orderBy(F.col("min_duration_sec").desc())
)
