from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "inner")
    .filter(F.col("b.browser") != "Chrome")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.duration_sec",
        F.col("b.country"),
    )
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.col("sum_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").desc_nulls_last())
)
