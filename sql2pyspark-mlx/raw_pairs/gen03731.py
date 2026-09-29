from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("session_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id", "country").orderBy("last_known_duration_sec", "id")
w4 = Window.orderBy(F.col("last_known_duration_sec").desc(), "id")

base = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-05-20").cast("date"))
    .select(
        "*",
        F.when(F.col("event_type").isin(["search", "page_view"]), "Group A").otherwise("Other").alias("event_type_group"),
        F.lower("event_type").alias("event_type_lower"),
    )
)

metrics = (
    base
    .select(
        "id",
        "session_id",
        "event_type_group",
        "event_type_lower",
        F.max("duration_sec").over(w_ordered).alias("running_max_duration_sec"),
        F.avg("duration_sec").over(w_ordered).alias("running_avg_duration_sec"),
        F.min("duration_sec").over(w_ordered).alias("running_min_duration_sec"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
    )
)

valid = (
    metrics
    .filter(F.col("running_avg_duration_sec") > 130)
    .select(
        "id",
        "session_id",
        "event_type_group",
        "event_type_lower",
        "running_max_duration_sec",
        "running_avg_duration_sec",
        "running_min_duration_sec",
        "last_known_duration_sec",
        F.col("last_known_duration_sec").cast("double").alias("last_known_duration_sec_num"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.session_id",
        "s.event_type_lower",
        "s.running_max_duration_sec",
        "s.last_known_duration_sec",
        F.col("d.country"),
    )
)

enriched = (
    joined
    .select(
        "id",
        "running_max_duration_sec",
        "country",
        F.lag("last_known_duration_sec").over(w_hist).alias("prev_last_known_duration_sec"),
        F.lag("running_max_duration_sec").over(w_hist).alias("prev_running_max_duration_sec"),
        F.lead("running_max_duration_sec").over(w_hist).alias("next_running_max_duration_sec"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.ntile(4).over(w4).alias("last_known_duration_sec_bucket"),
    )
)

filtered = (
    enriched
    .filter(F.col("next_running_max_duration_sec") < 270)
    .select(
        "id",
        "running_max_duration_sec",
        "prev_last_known_duration_sec",
        "prev_running_max_duration_sec",
        "row_seq",
        "last_known_duration_sec_bucket",
        (F.col("prev_running_max_duration_sec") * F.col("running_max_duration_sec")).alias("prev_running_max_duration_sec_times_running_max_duration_sec"),
    )
)

grouped = (
    filtered
    .withColumn("prev_running_max_duration_sec_times_running_max_duration_sec_tier", F.when(F.col("prev_running_max_duration_sec_times_running_max_duration_sec") >= 77, "PREMIUM").otherwise("STANDARD"))
    .withColumn("prev_running_max_duration_sec_tier", F.when(F.col("prev_running_max_duration_sec") >= 370, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["prev_running_max_duration_sec_times_running_max_duration_sec_tier", "prev_running_max_duration_sec_tier"], ["prev_running_max_duration_sec_times_running_max_duration_sec_tier"], []],
        "prev_running_max_duration_sec_times_running_max_duration_sec_tier",
        "prev_running_max_duration_sec_tier",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("last_known_duration_sec_bucket", 0.95).alias("last_known_duration_sec_bucket_p95"),
        F.percentile_approx("prev_running_max_duration_sec_times_running_max_duration_sec", [0.25, 0.5, 0.75]).alias("prev_running_max_duration_sec_times_running_max_duration_sec_quartiles"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("last_known_duration_sec_bucket"), 2).alias("avg_last_known_duration_sec_bucket"),
    )
    .filter(F.col("avg_last_known_duration_sec_bucket") > 0)
)

result = (
    grouped
    .select(
        "prev_running_max_duration_sec_times_running_max_duration_sec_tier",
        "prev_running_max_duration_sec_tier",
        "distinct_ids",
        "prev_running_max_duration_sec_times_running_max_duration_sec_quartiles",
        "avg_last_known_duration_sec_bucket",
    )
    .orderBy("prev_running_max_duration_sec_times_running_max_duration_sec_tier")
)
