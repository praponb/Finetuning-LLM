from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser", "device").orderBy(F.col("duration_sec").desc_nulls_last(), "id")
w2 = Window.partitionBy("device").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("device").orderBy("duration_sec", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("browser").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("browser")
w6 = Window.partitionBy("event_type_length_avg_5_tier").orderBy(F.col("event_type_length_per_desktop").desc())

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-05-06 01:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .withColumn("event_type_length", F.length("event_type"))
)

joined = (
    cleaned
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.session_id",
        "s.duration_sec",
        "s.event_type_length",
        F.col("d.browser"),
        F.col("d.device"),
    )
)

deduped = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .withColumn("last7_max_event_type_length", F.max("event_type_length").over(w2))
    .withColumn("last7_count_event_type_length", F.count("event_type_length").over(w2))
    .withColumn("last7_sum_event_type_length", F.sum("event_type_length").over(w2))
    .withColumn("last7_min_event_type_length", F.min("event_type_length").over(w2))
    .withColumn("event_type_length_avg_5", F.avg("event_type_length").over(w3))
)

with_windows = (
    metrics
    .select(
        "id",
        "session_id",
        "event_type_length",
        "browser",
        "device",
        "rank_in_group",
        "last7_max_event_type_length",
        "last7_sum_event_type_length",
        "last7_min_event_type_length",
        "event_type_length_avg_5",
        F.sum("last7_count_event_type_length").over(w4).alias("running_sum_last7_count_event_type_length"),
        F.max("last7_count_event_type_length").over(w4).alias("running_max_last7_count_event_type_length"),
        F.avg("last7_count_event_type_length").over(w4).alias("running_avg_last7_count_event_type_length"),
        F.max("last7_sum_event_type_length").over(w5).alias("browser_max_last7_sum_event_type_length"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_type_length_avg_5") >= 82, "PREMIUM").otherwise("STANDARD").alias("event_type_length_avg_5_tier"),
        F.when(F.col("last7_sum_event_type_length") >= 330, "PREMIUM").otherwise("STANDARD").alias("last7_sum_event_type_length_tier"),
        "browser",
    )
    .agg(
        F.size(F.collect_set("device")).alias("num_devices"),
        F.percentile_approx("running_sum_last7_count_event_type_length", 0.95).alias("running_sum_last7_count_event_type_length_p95"),
        F.round(F.sum("event_type_length") / F.nullif(F.count(F.when(F.col("device") == "desktop", 1)), F.lit(0)), 2).alias("event_type_length_per_desktop"),
        F.sort_array(F.collect_set("device")).alias("device_list"),
    )
    .withColumn("event_type_length_per_desktop_dense_rank", F.dense_rank().over(w6))
)
