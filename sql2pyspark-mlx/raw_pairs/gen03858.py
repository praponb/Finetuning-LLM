from pyspark.sql import functions as F

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "inner")
    .filter(F.col("p2.device").isin(["mobile", "tablet", "desktop"]))
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.event_time",
        F.col("p2.device"),
        F.col("p2.browser"),
        F.col("p2.country"),
    )
)

result = (
    with_dims
    .rollup("country", "session_id")
    .agg(
        F.max("event_time").alias("ended_at"),
        F.min("event_time").alias("first_seen"),
        F.grouping("country").alias("gid"),
    )
)
