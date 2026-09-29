from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page").isin(["/search", "/cart", "/home"]))
    .groupBy("session_id")
    .agg(F.sum("duration_sec").alias("total_duration_sec"))
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.total_duration_sec") > 1700)
    .select(
        F.col("s.id"),
        F.col("s.device"),
        F.col("s.browser"),
        F.col("webs.total_duration_sec"),
    )
    .orderBy(F.col("total_duration_sec").desc())
)
