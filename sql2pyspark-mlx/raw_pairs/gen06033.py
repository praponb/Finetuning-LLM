from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy(F.date_trunc("MONTH", "event_time"), "page").orderBy("duration_sec", "id")
w_grp = Window.orderBy("prev_duration_sec_rounded", "id")
w4 = Window.partitionBy("event_type", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy("prev_duration_sec_rounded", "id")
w6 = Window.partitionBy("event_type", "page").orderBy("running_avg_duration_sec", "id")
w7 = Window.partitionBy("page")
w8 = Window.partitionBy("event_type", "page").orderBy(F.col("running_sum_duration_sec_count_30d").desc_nulls_last())
w_ordered = Window.orderBy(F.col("prev_duration_sec_rounded").desc())

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-08-06").cast("date"))
    .withColumn("duration_sec_rounded", F.round("duration_sec", 0))
    .withColumn("page_clean", F.coalesce("page", F.lit("NONE")))
)

enriched = (
    cleaned
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "page_clean",
        F.avg("duration_sec").over(w).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w).alias("running_min_duration_sec"),
        F.sum("duration_sec").over(w).alias("running_sum_duration_sec"),
        F.lag("duration_sec_rounded", 1, 0).over(w2).alias("prev_duration_sec_rounded"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.lag("prev_duration_sec_rounded", 1, 0).over(w_grp).alias("prev_prev_duration_sec_rounded"),
        F.ntile(4).over(w_grp).alias("prev_duration_sec_rounded_bucket"),
        F.count("running_sum_duration_sec").over(w4).alias("running_sum_duration_sec_count_30d"),
        F.ntile(5).over(w5).alias("prev_duration_sec_rounded_bucket_2"),
        F.ntile(5).over(w6).alias("running_avg_duration_sec_bucket"),
    )
)

remaining_keys = (
    scored
    .select("id", "event_type")
    .exceptAll(
        df_web_events
        .filter(F.col("event_type") == "page_view")
        .select("id", "event_type")
    )
)

reconciled = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.event_type") == F.col("k.event_type")), "inner")
    .select("s.*")
)

with_windows = (
    reconciled
    .select(
        "id",
        "event_type",
        "event_time",
        "running_sum_duration_sec",
        "prev_duration_sec_rounded",
        "prev_prev_duration_sec_rounded",
        "prev_duration_sec_rounded_bucket",
        "running_sum_duration_sec_count_30d",
        "prev_duration_sec_rounded_bucket_2",
        F.round((F.col("running_sum_duration_sec_count_30d") / F.sum("running_sum_duration_sec_count_30d").over(w7)), 4).alias("running_sum_duration_sec_count_30d_share"),
        F.rank().over(w8).alias("running_sum_duration_sec_count_30d_rank"),
    )
)

valid = (
    with_windows
    .filter(F.col("prev_duration_sec_rounded").between(54, 58))
    .withColumn("prev_duration_sec_rounded_bucket_2_str", F.col("prev_duration_sec_rounded_bucket_2").cast("double"))
)

metrics = (
    valid
    .select(
        "event_time",
        "prev_duration_sec_rounded",
        "prev_prev_duration_sec_rounded",
        "prev_duration_sec_rounded_bucket",
        "running_sum_duration_sec_count_30d",
        "prev_duration_sec_rounded_bucket_2",
        "running_sum_duration_sec_count_30d_share",
        "running_sum_duration_sec_count_30d_rank",
        "prev_duration_sec_rounded_bucket_2_str",
        F.cume_dist().over(w_ordered).alias("prev_duration_sec_rounded_cume_dist"),
    )
)

daily = (
    metrics
    .withColumn("prev_prev_duration_sec_rounded_tier", F.when(F.col("prev_prev_duration_sec_rounded") >= 87, "PREMIUM").otherwise("STANDARD"))
    .withColumn("prev_duration_sec_rounded_bucket_tier", F.when(F.col("prev_duration_sec_rounded_bucket") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_prev_duration_sec_rounded_tier", "prev_duration_sec_rounded_bucket_tier"], ["prev_prev_duration_sec_rounded_tier"], []],
        "prev_prev_duration_sec_rounded_tier",
        "prev_duration_sec_rounded_bucket_tier",
    )
    .agg(
        F.max("running_sum_duration_sec_count_30d").alias("max_running_sum_duration_sec_count_30d"),
        F.percentile_approx("prev_prev_duration_sec_rounded", 0.95).alias("prev_prev_duration_sec_rounded_p95"),
        F.max("event_time").alias("last_seen"),
        F.max("event_time").alias("last_seen_2"),
        F.count("*").alias("row_count"),
    )
)

result = (
    daily
    .select(
        "prev_prev_duration_sec_rounded_tier",
        "prev_duration_sec_rounded_bucket_tier",
        "max_running_sum_duration_sec_count_30d",
        "prev_prev_duration_sec_rounded_p95",
        "last_seen",
        "last_seen_2",
        "row_count",
    )
)
