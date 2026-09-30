from pyspark.sql import functions as F

base = (
    df_web_events
    .filter((F.col("event_time") >= F.to_timestamp(F.lit("2023-07-06 12:00:00"))) & F.col("page").isNull())
    .select(
        "*",
        F.substring("event_type", 1, 4).alias("event_type_prefix"),
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("resolved_page"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .select("src.*", F.col("d.browser"), F.col("d.device"))
)

kept_keys = (
    joined
    .select("event_type", "session_id")
    .subtract(
        df_web_events
        .filter(F.col("page").isin(["/product", "/cart"]))
        .select("event_type", "session_id")
    )
)

verified = (
    joined
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.session_id") == F.col("k.session_id")), "inner")
    .select("s.*")
)

cleaned = (
    verified
    .filter((F.col("event_time") >= F.lit("2023-06-19 11:00:00").cast("timestamp")) & F.col("browser").isNotNull())
    .withColumn("device_upper", F.upper("device"))
)

joined_web_events = (
    cleaned
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Chrome", "Edge"]))
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_type_prefix",
        "a.resolved_page",
        "a.browser",
        "a.device_upper",
        F.col("s.device").alias("session_device"),
    )
)

cleaned_web_events = (
    joined_web_events
    .filter(F.col("page").isNotNull())
    .select(
        "session_id",
        "page",
        "event_type_prefix",
        "resolved_page",
        "browser",
        "device_upper",
        "session_device",
        F.upper("session_device").alias("session_device_upper"),
        F.concat_ws(",", "session_device", "page").alias("session_device_page_key"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_clean"),
    )
)

result = (
    cleaned_web_events
    .select(
        "page",
        "event_type_prefix",
        "browser",
        "device_upper",
        "session_device_upper",
        "session_device_page_key",
        "page_clean",
    )
    .orderBy("page")
)
