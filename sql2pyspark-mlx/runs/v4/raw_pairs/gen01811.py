from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.sum("duration_sec").alias("sum_web_event_duration_sec"))

remaining_keys = (
    df_web_events
    .select("page")
    .intersectAll(
        df_web_events
        .filter(F.col("event_time") < "2024-01-01")
        .select("page")
    )
)

reconciled = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select("session_id", "page", "event_time", F.col("sum_web_event_duration_sec"))
    .orderBy(F.col("session_id").desc_nulls_first())
)
