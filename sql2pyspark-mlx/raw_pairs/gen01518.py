from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .filter(F.col("page") == "/home")
    .groupBy("session_id")
    .agg(
        F.count("duration_sec").alias("duration_sec_count"),
        F.countDistinct("duration_sec").alias("unique_pages"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "inner")
    .filter(F.col("webs.unique_pages") >= 7)
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("webs.duration_sec_count"),
        F.col("webs.unique_pages"),
    )
    .orderBy(F.col("duration_sec_count").desc(), F.col("unique_pages").desc())
)
