from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.sort_array(F.collect_set("page")).alias("page_list"),
        (F.max("duration_sec") - F.min("duration_sec")).alias("duration_sec_range"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.duration_sec_range") > 24)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("webs.page_list"),
        F.col("webs.duration_sec_range"),
    )
)
