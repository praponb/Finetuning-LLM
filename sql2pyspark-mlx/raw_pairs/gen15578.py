from pyspark.sql import functions as F

base = (
    df_web_events
    .filter(F.col("page") == "/cart")
    .withColumn("duration_sec_level", F.when(F.col("duration_sec") >= 521, "High").when(F.col("duration_sec") >= 143, "Medium").otherwise("Low"))
    .withColumn("clean_event_type", F.trim("event_type"))
)

with_dims = (
    base
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.customer_id").isin([7, 17, 21, 24]))
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.duration_sec_level",
        "s.clean_event_type",
        F.col("p.device"),
        F.col("p.country"),
        F.col("p.browser"),
    )
)

enriched = (
    with_dims
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.browser").alias("session_browser"),
        F.col("p.device").alias("session_device"),
        F.col("p.country").alias("session_country"),
    )
)

joined = (
    enriched
    .alias("s")
    .join(df_sessions.alias("b"), F.col("s.session_id") == F.col("b.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.duration_sec_level",
        "s.device",
        "s.browser",
        "s.session_device",
        "s.session_country",
        F.col("b.country").alias("session_country_2"),
        F.col("b.device").alias("session_device_2"),
    )
)

cleaned = (
    joined
    .filter(F.col("device").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec_level",
        "session_country",
        F.concat_ws("-", "session_country", "device").alias("session_country_device_key"),
        F.concat("session_device", F.lit(" - "), "device").alias("session_device_device"),
    )
)

with_dims_web_events = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("p2"), F.col("s.session_id") == F.col("p2.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.duration_sec_level",
        "s.session_country",
        "s.session_country_device_key",
        F.col("p2.browser"),
    )
)

result = (
    with_dims_web_events
    .groupBy("browser")
    .agg(
        F.count_if(F.col("session_country") == "DE").alias("de_events"),
        F.count_if(F.col("browser") == "Firefox").alias("firefox_events"),
        F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_rows"),
    )
    .orderBy(F.col("de_events").desc())
)
