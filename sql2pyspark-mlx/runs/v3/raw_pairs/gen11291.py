from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.customer_id") == 3)
    .groupBy(F.col("s.device"))
    .agg(
        F.count(F.col("we.id")).alias("web_event_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
    )
    .orderBy("device")
)
