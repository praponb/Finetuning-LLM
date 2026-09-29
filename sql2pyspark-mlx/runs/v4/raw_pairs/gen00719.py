from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.to_date(F.lit("2024-02-19"))) & F.col("page").isNotNull() & F.hour("event_time").between(10, 17))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.concat_ws("/", "page", "event_type").alias("page_event_type_key"),
    )
)

valid = (
    filtered
    .filter((F.col("event_time") > F.to_date(F.lit("2023-03-31"))) & F.col("page").isNotNull() & F.hour("event_time").between(9, 13))
    .select(
        "*",
        F.length("page").alias("page_length"),
        F.datediff(F.current_date(), "event_time").alias("days_since_event_time"),
        F.coalesce("page", F.lit("NONE")).alias("page_clean"),
    )
)

cleaned = (
    valid
    .filter(F.col("page").isNotNull() & F.col("session_id").isin([21, 30]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "page_event_type_key",
        "page_clean",
        F.trim("page").alias("clean_page"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

enriched = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.browser"), F.col("b.device"))
)

result = (
    enriched
    .groupBy("device")
    .agg(
        F.count("*").alias("event_count"),
        F.size(F.collect_set("device")).alias("num_devices"),
    )
)
