from pyspark.sql import functions as F

duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.max("duration_sec").alias("max_web_event_duration_sec"))

base = (
    df_web_events
    .filter(F.hour("event_time").between(11, 15))
    .select(
        "*",
        F.concat_ws("-", "event_type", "page").alias("event_type_page_key"),
        F.length("event_type").alias("event_type_length"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

cleaned = (
    base
    .filter((F.col("event_time") > F.lit("2023-07-09").cast("date")) & F.col("session_id").isin([10, 40]))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "event_type_page_key",
        "page_clean",
        F.col("duration_sec").cast("double").alias("duration_sec_num"),
        F.round((F.col("event_type_length") * 1.15), 2).alias("event_type_length_scaled"),
    )
)

remaining_keys = (
    cleaned
    .select("id", "session_id")
    .subtract(
        df_web_events
        .filter(F.to_date("event_time") < "2024-08-01")
        .select("id", "session_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

with_dims = (
    kept_rows
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.started_at") >= "2024-10-21")
    .select("a.session_id", "a.event_type_page_key", F.col("s.device"))
)

matched_keys = (
    with_dims
    .select("session_id")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/checkout", "/product", "/cart"]))
        .select("session_id")
    )
)

reconciled = (
    with_dims
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

filtered = (
    reconciled
    .filter(F.col("device").isNotNull())
    .select(
        "*",
        F.lower("device").alias("device_lower"),
        F.coalesce("device", F.lit("N/A")).alias("device_clean"),
    )
)

base_web_events = (
    filtered
    .filter(F.col("device").isNotNull())
    .select(
        "session_id",
        "event_type_page_key",
        "device",
        "device_lower",
        "device_clean",
        F.when(F.col("device") == "tablet", 1).otherwise(0).alias("is_tablet"),
        F.coalesce("device", F.lit("OTHER")).alias("device_clean_2"),
    )
)

result = (
    base_web_events
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "session_id",
        "device_clean",
        "device_clean_2",
        F.col("max_web_event_duration_sec"),
    )
    .orderBy(F.col("session_id").asc_nulls_last())
)
