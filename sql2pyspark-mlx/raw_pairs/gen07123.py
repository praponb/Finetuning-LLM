from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page").isNotNull() & ~F.col("event_type").isin(["page_view", "purchase", "search"]))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("page").isin(["/home", "/product"]), "Priority").otherwise("Other").alias("page_group"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

enriched = (
    base
    .alias("a")
    .join(df_sessions.alias("b"), F.col("a.session_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.page_filled",
        F.col("b.browser"),
        F.col("b.device"),
    )
)

kept_keys = (
    enriched
    .select("id")
    .subtract(
        df_web_events
        .filter(F.col("event_time") >= "2024-01-01")
        .select("id")
    )
)

kept_rows = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base_web_events = (
    kept_rows
    .filter((F.col("event_time") > F.lit("2023-06-21").cast("date")) & F.col("device").isNotNull())
    .select(
        "session_id",
        "event_time",
        "device",
        F.trim("device").alias("clean_device"),
    )
)

joined = (
    base_web_events
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .select(
        "a.session_id",
        "a.event_time",
        "a.device",
        "a.clean_device",
        F.col("s.country"),
        F.col("s.browser"),
        F.col("s.device").alias("session_device"),
    )
)

enriched_web_events = (
    joined
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "US")
    .select(
        "e.*",
        F.col("s.device").alias("session_device_2"),
        F.col("s.browser").alias("session_browser"),
        F.col("s.country").alias("session_country"),
    )
)

result = (
    enriched_web_events
    .groupingSets(
        [["session_device_2", "session_device"], ["session_device_2"], []],
        "session_device_2",
        "session_device",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("session_browser").alias("distinct_session_browsers"),
        F.count(F.when(F.col("session_browser") == "Safari", 1)).alias("safari_count"),
        F.grouping("session_device_2").alias("grouping_level"),
    )
    .filter(F.col("safari_count") > 1)
)
