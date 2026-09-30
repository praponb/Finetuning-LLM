from pyspark.sql import functions as F

cleaned = (
    df_web_events
    .filter(F.hour("event_time").between(6, 11))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.date_trunc("MONTH", "event_time").alias("event_time_period"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_sessions.alias("p"), F.col("src.session_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.session_id",
        "src.event_type",
        "src.page",
        "src.event_time",
        "src.duration_sec",
        "src.event_time_period",
        "src.page_filled",
        F.col("p.browser"),
    )
)

filtered = (
    enriched
    .filter(F.col("browser").isNotNull() & (F.col("page") == "/search"))
    .withColumn("event_time_quarter", F.quarter("event_time"))
    .withColumn("browser_upper", F.upper("browser"))
)

kept_keys = (
    filtered
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(138, 266))
        .select("page", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("year_start", F.date_trunc("YEAR", "event_time_period"))
    .groupingSets(
        [["year_start", "event_type"], ["year_start"], []],
        "year_start",
        "event_type",
    )
    .agg(
        F.sort_array(F.collect_set("event_type")).alias("event_type_list"),
        F.round(F.avg("event_time_quarter"), 2).alias("avg_event_time_quarter"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_type").asc_nulls_last())
)
