from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("page"))).alias("page_values"),
        F.sort_array(F.collect_set("page")).alias("page_list"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .select(F.col("s.browser"), F.col("webs.page_values"), F.col("webs.page_list"))
    .orderBy("browser", "page_values")
)
