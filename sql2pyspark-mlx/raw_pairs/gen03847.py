from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("we.event_time") < "2023-12-01")
    .groupBy(F.col("s.browser"))
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set(F.col("we.page"))).alias("page_list"),
    )
)
