from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter((F.col("event_time") > F.to_timestamp(F.lit("2023-03-26 07:00:00"))) & F.col("page").isNotNull())
    .select("*", F.col("duration_sec").cast("string").alias("duration_sec_num"))
)

enriched = (
    valid
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.customer_id").isNull())
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.duration_sec_num",
        F.col("d.browser"),
        F.col("d.country"),
        F.col("d.device"),
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
        "s.page",
        "s.duration_sec_num",
        "s.country",
        F.col("b.country").alias("session_country"),
        F.col("b.browser").alias("session_browser"),
    )
)

filtered = (
    joined
    .filter(F.col("session_country").isNotNull() & ~F.col("country").isin(["TH", "JP", "SG"]))
    .withColumn("event_type_page", F.concat("event_type", F.lit(" "), "page"))
    .withColumn("page_upper", F.upper("page"))
)

filtered_web_events = (
    filtered
    .filter(F.col("session_browser").isNotNull())
    .withColumn("clean_event_type", F.trim("event_type"))
    .withColumn("resolved_page", F.coalesce("page", F.lit("UNKNOWN")))
)

result = (
    filtered_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec_num",
        "session_country",
        "session_browser",
        "page_upper",
        "clean_event_type",
        "resolved_page",
    )
    .orderBy("id")
)
