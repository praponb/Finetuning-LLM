from pyspark.sql import functions as F

filtered = (
    df_web_events
    .filter((F.col("event_time") > F.lit("2023-05-07").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "duration_sec",
        F.when(F.col("page").isin(["/search", "/cart"]), "Priority").otherwise("Other").alias("page_group"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"))
)

matched_keys = (
    enriched
    .select("event_type", "id")
    .intersect(
        df_web_events
        .filter(F.col("session_id").isin([16, 24, 31]))
        .select("event_type", "id")
    )
)

verified = (
    enriched
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.event_type") == F.col("k.event_type")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_sessions.alias("p"), F.col("e.session_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.session_id",
        "e.event_type",
        "e.page",
        "e.duration_sec",
        "e.country",
        F.col("p.browser"),
        F.col("p.device"),
        F.col("p.country").alias("session_country"),
    )
)

result = (
    joined
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.browser").alias("session_browser"), F.col("a.page").alias("page"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.duration_sec")), 2).alias("total_duration_sec"),
    )
)
