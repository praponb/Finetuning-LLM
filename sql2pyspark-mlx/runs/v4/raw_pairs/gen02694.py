from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.country"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_sessions"),
    )
    .orderBy("id")
)
