from pyspark.sql import functions as F

valid = (
    df_web_events
    .filter(F.col("page").isin(["/home", "/product", "/cart"]) & (F.col("event_time") >= F.to_timestamp(F.lit("2023-08-01 16:00:00"))))
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.when(F.col("duration_sec") >= 387, "High").when(F.col("duration_sec") >= 144, "Medium").otherwise("Low").alias("duration_sec_tier"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

remaining_keys = (
    valid
    .select("page", "event_type")
    .intersect(
        df_web_events
        .filter(F.col("event_type").isin(["add_to_cart", "click"]))
        .select("page", "event_type")
    )
)

reconciled = (
    valid
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

kept_keys = (
    reconciled
    .select("id", "page")
    .intersectAll(
        df_web_events
        .filter(F.to_date("event_time") < "2025-10-01")
        .select("id", "page")
    )
)

verified = (
    reconciled
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.page") == F.col("k.page")), "inner")
    .select("s.*")
)

with_dims = (
    verified
    .alias("src")
    .join(df_sessions.alias("d"), F.col("src.session_id") == F.col("d.id"), "left")
    .filter(F.col("d.device") == "mobile")
    .select("src.*", F.col("d.browser"), F.col("d.country"))
)

stats = (
    with_dims
    .groupBy(
        F.when(F.col("duration_sec") >= 537, "PREMIUM").otherwise("STANDARD").alias("duration_sec_tier"),
    )
    .agg(
        F.avg(F.when(F.col("country") == "SG", F.col("duration_sec"))).alias("sg_avg_duration_sec"),
        F.size(F.collect_set("event_type")).alias("num_event_types"),
        F.countDistinct("page").alias("distinct_pages"),
    )
)

result = (
    stats
    .select(
        "duration_sec_tier",
        "sg_avg_duration_sec",
        "num_event_types",
        "distinct_pages",
    )
    .orderBy(F.col("num_event_types").desc(), F.col("duration_sec_tier").desc())
)
