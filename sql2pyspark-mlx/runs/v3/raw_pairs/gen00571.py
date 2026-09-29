from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "outer")
    .groupBy(F.to_date(F.col("s.started_at")).alias("started_day"))
    .agg(
        F.sum(F.when(F.col("we.page") == "/search", F.col("we.duration_sec")).otherwise(0)).alias("search_duration_sec"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
    )
)
