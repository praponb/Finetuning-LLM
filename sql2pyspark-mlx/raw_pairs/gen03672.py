from pyspark.sql import functions as F

web_events_by_session = (
    df_web_events
    .groupBy("session_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("page") == "/checkout", 1).otherwise(0))) / F.count("*")), 1).alias("pct_checkout"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(web_events_by_session.alias("webs"), F.col("s.id") == F.col("webs.session_id"), "left")
    .filter(F.col("webs.pct_checkout") > 69)
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("webs.pct_checkout"),
        F.col("webs.stddev_duration_sec"),
    )
    .orderBy(F.col("stddev_duration_sec").desc(), F.col("country").desc())
)
