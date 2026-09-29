from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "right")
    .filter(F.col("we.page") != "/search")
    .groupBy(F.col("s.browser"), F.year(F.col("s.started_at")).alias("started_year"))
    .agg(
        F.countDistinct(F.col("we.duration_sec")).alias("unique_event_types"),
        F.round(F.stddev(F.col("we.duration_sec")), 2).alias("stddev_duration_sec"),
    )
    .filter(F.col("unique_event_types") > 3)
)
