from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("country", "device").orderBy(F.col("duration_sec").desc(), "id")
w4 = Window.orderBy(F.col("duration_sec_bucket").desc(), "id")
w5 = Window.orderBy("duration_sec", "id").rowsBetween(-2, -1)
w6 = Window.orderBy("duration_sec_bucket_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("browser").orderBy("last4_count_duration_sec_avg_2_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("device").orderBy("duration_sec_bucket_bucket")

with_windows = (
    df_web_events
    .select(
        "*",
        F.count("duration_sec").over(w).alias("last4_count_duration_sec"),
        F.sum("duration_sec").over(w).alias("last4_sum_duration_sec"),
        F.avg("duration_sec").over(w).alias("last4_avg_duration_sec"),
        F.ntile(3).over(w2).alias("duration_sec_bucket"),
    )
)

enriched = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type",
        "s.page",
        "s.duration_sec",
        "s.last4_count_duration_sec",
        "s.duration_sec_bucket",
        F.col("p.browser"),
        F.col("p.country"),
        F.col("p.device"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "event_type",
        "page",
        "browser",
        "country",
        "device",
        F.ntile(3).over(w3).alias("duration_sec_bucket_2"),
        F.ntile(10).over(w4).alias("duration_sec_bucket_bucket"),
        F.avg("last4_count_duration_sec").over(w5).alias("last4_count_duration_sec_avg_2_prior"),
    )
)

with_windows_web_events = (
    metrics
    .select(
        "id",
        "page",
        "browser",
        "country",
        "duration_sec_bucket_bucket",
        F.sum("last4_count_duration_sec_avg_2_prior").over(w6).alias("cumulative_last4_count_duration_sec_avg_2_prior"),
        F.last("device", ignorenulls=True).over(w7).alias("last_known_device"),
        F.rank().over(w8).alias("duration_sec_bucket_bucket_rank"),
    )
)

valid = (
    with_windows_web_events
    .filter(F.col("country").isNotNull() & (F.col("page") == "/product"))
    .select(
        "id",
        "page",
        "browser",
        "duration_sec_bucket_bucket",
        "last_known_device",
        "duration_sec_bucket_bucket_rank",
        F.lower("country").alias("country_lower"),
    )
)

result = (
    valid
    .select(
        "id",
        "page",
        "duration_sec_bucket_bucket",
        "last_known_device",
        "duration_sec_bucket_bucket_rank",
    )
)
