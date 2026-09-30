from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("we.page").isin(["/home", "/search", "/cart"]))
    .groupBy(F.col("s.browser"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.event_type")))).alias("event_type_values"),
        F.max(F.col("we.event_time")).alias("latest_event_time"),
    )
    .orderBy("browser")
)
