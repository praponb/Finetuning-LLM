from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.browser"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.max(F.col("we.event_time")).alias("last_event_time"),
        F.sort_array(F.collect_set(F.col("we.event_type"))).alias("event_type_list"),
    )
    .orderBy(F.col("id").desc(), "browser")
    .limit(10)
)
