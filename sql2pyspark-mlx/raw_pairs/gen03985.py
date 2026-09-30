from pyspark.sql import functions as F

base = (
    df_sessions
    .filter((F.col("started_at") > F.lit("2023-08-31 03:00:00").cast("timestamp")) & F.col("customer_id").isin([28, 31]))
    .select(
        "id",
        "device",
        F.initcap(F.trim("country")).alias("country_title"),
        F.coalesce("browser", F.lit("NONE")).alias("browser_filled"),
    )
)

kept_keys = (
    base
    .select("id")
    .exceptAll(
        df_sessions
        .filter(F.col("customer_id").isNotNull())
        .select("id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

not_flagged = (
    verified
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.event_time") < "2024-02-09"), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (~F.col("ch.page").isin(["/search", "/home", "/cart"])), "left_anti")
)

filtered = (
    kept
    .filter(F.col("device") == "mobile")
    .select("*", F.upper("device").alias("device_upper"))
)

not_flagged_sessions = (
    filtered
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec").between(153, 361)), "left_anti")
)

totals = (
    not_flagged_sessions
    .groupBy("device")
    .agg(
        F.countDistinct("device").alias("distinct_devices"),
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.count(F.when(F.col("device") == "mobile", 1)).alias("mobile_count"),
    )
)

result = totals.select("device")
