from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.duration_sec").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("browser").orderBy("web_event_event_time", "id")
w3 = Window.partitionBy("browser").orderBy("web_event_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "device",
        "browser",
        "country",
        F.when(F.col("device").isin(["tablet", "mobile"]), "Tier 1").otherwise("Other").alias("device_group"),
        F.coalesce("browser", F.lit("NONE")).alias("resolved_browser"),
    )
)

cleaned = (
    filtered
    .filter(F.col("browser").isNotNull() & (F.col("customer_id") == 28))
    .select(
        "*",
        F.trim("device").alias("clean_device"),
        F.substring("device", 1, 4).alias("device_prefix"),
        F.coalesce("browser", F.lit("UNKNOWN")).alias("resolved_browser_2"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.device",
        "s.browser",
        "s.device_group",
        "s.resolved_browser",
        "s.clean_device",
        "s.device_prefix",
        "s.resolved_browser_2",
        F.col("c.event_time").alias("web_event_event_time"),
        F.col("c.duration_sec").alias("web_event_duration_sec"),
    )
)

gaps = (
    latest_match
    .withColumn("session_start_flag", F.when(F.col("web_event_event_time") - F.lag("web_event_event_time").over(w2) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_sessions = (
    gaps
    .select(
        "id",
        "device",
        "browser",
        "device_prefix",
        "web_event_event_time",
        "web_event_duration_sec",
        F.sum("session_start_flag").over(w3).alias("burst_id"),
    )
)

not_flagged = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")) & (F.col("c.duration_sec") > 456), "left_semi")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_web_events.alias("ch"), (F.col("ch.session_id") == F.col("s.id")) & (F.col("ch.session_id").isin([16, 24, 26])), "left_anti")
)

base = (
    kept
    .filter((~F.col("browser").eqNullSafe(F.lit("Edge"))) & (F.col("web_event_event_time") < "2025-02-01"))
    .select(
        "id",
        "device",
        "web_event_duration_sec",
        "burst_id",
        F.trim("device").alias("clean_device"),
        F.coalesce("browser", F.lit("DIRECT")).alias("browser_clean"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("burst_id") >= 7, "PREMIUM").otherwise("STANDARD").alias("burst_id_tier"),
    )
    .agg(
        F.count(F.when(F.col("device") == "desktop", 1)).alias("desktop_count"),
        F.countDistinct("device").alias("distinct_devices"),
        F.percentile_approx("burst_id", 0.9).alias("burst_id_p90"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
        F.sort_array(F.collect_set("device")).alias("device_list_2"),
    )
)
