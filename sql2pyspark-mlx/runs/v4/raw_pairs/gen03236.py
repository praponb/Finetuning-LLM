from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(F.sort_array(F.collect_set("page")).alias("page_list"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .select(F.col("s.country"), F.col("webs.page_list"))
    .orderBy("country")
)
