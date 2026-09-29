from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("duration_sec")
w2 = Window.partitionBy("country").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(-2, Window.currentRow)

joined = (
    df_web_events
    .alias("a")
    .join(df_sessions.alias("p"), F.col("a.session_id") == F.col("p.id"), "left")
    .filter(F.col("p.browser") == "Edge")
    .select(
        "a.id",
        "a.session_id",
        "a.page",
        "a.event_time",
        "a.duration_sec",
        F.col("p.browser"),
        F.col("p.country"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "session_id",
        "event_time",
        "country",
        F.cume_dist().over(w).alias("duration_sec_cume_dist"),
        F.sum("duration_sec").over(w2).alias("running_duration_sec"),
    )
)

enriched = (
    with_windows
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.event_time",
        "src.country",
        "src.duration_sec_cume_dist",
        "src.running_duration_sec",
        F.col("s.country").alias("session_country"),
    )
)

cleaned = (
    enriched
    .filter(~F.col("country").isin(["SG", "DE"]) & F.col("country").isNotNull())
    .select(
        "id",
        "session_id",
        "event_time",
        "country",
        "duration_sec_cume_dist",
        F.coalesce("duration_sec_cume_dist", F.lit(0)).alias("duration_sec_cume_dist_filled"),
        F.coalesce("session_country", F.lit("OTHER")).alias("resolved_session_country"),
    )
)

with_windows_web_events = (
    cleaned
    .select(
        "id",
        "session_id",
        "event_time",
        "duration_sec_cume_dist",
        "duration_sec_cume_dist_filled",
        F.sum("duration_sec_cume_dist").over(w3).alias("last3_sum_duration_sec_cume_dist"),
        F.count("duration_sec_cume_dist").over(w3).alias("last3_count_duration_sec_cume_dist"),
        F.min("duration_sec_cume_dist").over(w3).alias("last3_min_duration_sec_cume_dist"),
    )
)

result = (
    with_windows_web_events
    .groupBy(
        "session_id",
        F.when(F.col("last3_min_duration_sec_cume_dist") >= 0.72, "LARGE").otherwise("SMALL").alias("last3_min_duration_sec_cume_dist_tier"),
    )
    .agg(
        F.min("event_time").alias("first_seen"),
        F.percentile_approx("last3_sum_duration_sec_cume_dist", 0.9).alias("last3_sum_duration_sec_cume_dist_p90"),
        F.countDistinct("session_id").alias("distinct_session_ids"),
        F.max("last3_min_duration_sec_cume_dist").alias("max_last3_min_duration_sec_cume_dist"),
    )
)
