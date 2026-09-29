from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

remaining_keys = (
    df_web_events
    .select("session_id", "page")
    .subtract(
        df_web_events
        .filter(F.col("event_type") == "add_to_cart")
        .select("session_id", "page")
    )
)

kept_rows = (
    df_web_events
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.when(F.col("event_type").rlike("^(add_to_cart|click)$"), "GROUP_B").otherwise("CORE").alias("event_type_zone"),
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy(F.col("event_type").desc_nulls_last(), "session_id")
)
