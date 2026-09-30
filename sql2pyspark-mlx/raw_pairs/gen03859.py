from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

with_dims = (
    df_web_events
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.device").isin(["tablet", "desktop"]))
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.page",
        "e.event_time",
        F.col("p.browser"),
        F.col("p.device"),
        F.col("p.country"),
    )
)

result = (
    with_dims
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_time",
        "device",
        "country",
        F.when(F.col("country").rlike("^(SG|UK)$"), "PRIORITY").otherwise("GROUP_A").alias("country_zone"),
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("device").asc_nulls_first())
)
