from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("page").orderBy("duration_sec", "id")
w2 = Window.partitionBy("page").orderBy("duration_sec_bucket", "id")
w_running = Window.partitionBy("page").orderBy("prev_duration_sec", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("session_id").orderBy(F.col("duration_sec_bucket").desc(), "id")
w5 = Window.partitionBy("page", "session_id").orderBy("duration_sec_bucket", "id")
w6 = Window.partitionBy("device", "session_id")

cleaned = (
    df_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= F.lit("2023-09-29 10:00:00").cast("timestamp")))
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        F.substring("page", 1, 3).alias("page_prefix"),
        F.coalesce("page", F.lit("OTHER")).alias("page_filled"),
    )
)

enriched = (
    cleaned
    .select(
        "id",
        "session_id",
        "page",
        "page_prefix",
        F.lag("duration_sec", 2).over(w_part).alias("prev_duration_sec"),
        F.ntile(10).over(w_part).alias("duration_sec_bucket"),
    )
)

enriched_web_events = (
    enriched
    .alias("e")
    .join(df_sessions.alias("s"), F.col("e.session_id") == F.col("s.id"), "inner")
    .filter(F.col("s.device").isin(["desktop", "tablet", "mobile"]))
    .select("e.*", F.col("s.device"))
)

with_windows = (
    enriched_web_events
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec_bucket",
        F.row_number().over(w2).alias("row_seq"),
        F.avg("duration_sec_bucket").over(w_running).alias("duration_sec_bucket_avg_2_prior"),
        F.ntile(3).over(w4).alias("duration_sec_bucket_bucket"),
        F.ntile(4).over(w5).alias("duration_sec_bucket_bucket_2"),
        F.round((F.col("duration_sec_bucket") / F.sum("duration_sec_bucket").over(w6)), 4).alias("duration_sec_bucket_share"),
    )
)

base = (
    with_windows
    .filter(F.col("page").isNotNull() & (F.col("duration_sec_bucket_bucket_2") > 6))
    .select(
        "id",
        "page",
        "duration_sec_bucket",
        "duration_sec_bucket_avg_2_prior",
        "duration_sec_bucket_bucket",
        "duration_sec_bucket_bucket_2",
        "duration_sec_bucket_share",
        F.round("duration_sec_bucket_share", 0).alias("duration_sec_bucket_share_rounded"),
        F.least("duration_sec_bucket_bucket_2", "duration_sec_bucket_avg_2_prior").alias("least_duration_sec_bucket_bucket_2_duration_sec_bucket_avg_2_prior"),
        F.coalesce("page", F.lit("N/A")).alias("page_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("duration_sec_bucket_bucket") >= 7, "LARGE").otherwise("SMALL").alias("duration_sec_bucket_bucket_tier"),
    )
    .agg(
        F.percentile_approx("duration_sec_bucket", 0.95).alias("duration_sec_bucket_p95"),
        F.count_if(F.col("page") == "/checkout").alias("checkout_events"),
        F.round(F.avg("duration_sec_bucket_bucket_2"), 2).alias("avg_duration_sec_bucket_bucket_2"),
    )
    .filter(F.col("avg_duration_sec_bucket_bucket_2") >= 0)
)
