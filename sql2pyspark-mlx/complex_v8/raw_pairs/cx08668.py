from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("track_id", "is_skipped_label").orderBy(F.col("ms_played_bucket").desc(), F.col("id").desc())
w_listener_id = Window.partitionBy("listener_id")
w_all_by_max_ms_played_bucket = Window.orderBy(F.col("max_ms_played_bucket").desc(), "id")
w_track_id_by_ms_played_bucket = Window.partitionBy("track_id").orderBy("ms_played_bucket_minus_max", "id")

base_plays = (
    df_plays
    .filter((F.col("source").isNotNull()) | (F.col("listener_id") <= 9))
    .select(
        "id",
        "listener_id",
        "track_id",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
        F.substring(F.trim(F.col("device")), 1, 3).alias("device_prefix"),
    )
)

device_stats = (
    base_plays
    .groupBy("device")
    .agg(F.max("ms_played_bucket").alias("max_ms_played_bucket"))
)

above_average = (
    base_plays.alias("s")
    .join(device_stats.alias("g"), F.col("g.device") == F.col("s.device"), "inner")
    .filter(F.col("s.ms_played_bucket") == F.col("g.max_ms_played_bucket"))
    .select(
        "s.*",
        "g.max_ms_played_bucket",
        F.round(F.col("s.ms_played_bucket") - F.col("g.max_ms_played_bucket"), 2).alias("ms_played_bucket_minus_max"),
    )
)

without_matches = (
    above_average.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
    .filter(F.col("device") == "car")
)

latest = (
    without_matches
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .select(
        "id",
        "listener_id",
        "source",
        "ms_played_bucket",
        "is_skipped_label",
        "device_prefix",
        "max_ms_played_bucket",
        "ms_played_bucket_minus_max",
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.lead("ms_played_bucket_minus_max").over(w_all_by_max_ms_played_bucket).alias("next_ms_played_bucket_minus_max"),
        F.sum("max_ms_played_bucket").over(w_listener_id).alias("listener_id_sum_max_ms_played_bucket"),
        F.lag("ms_played_bucket", 3).over(w_all_by_max_ms_played_bucket).alias("prev3_ms_played_bucket"),
        F.ntile(4).over(w_track_id_by_ms_played_bucket).alias("ms_played_bucket_minus_max_quartile"),
    )
)

with_details_listener = (
    with_windows.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.source",
        "s.is_skipped_label",
        "s.device_prefix",
        "s.max_ms_played_bucket",
        "s.ms_played_bucket_minus_max",
        "s.next_ms_played_bucket_minus_max",
        "s.listener_id_sum_max_ms_played_bucket",
        "s.prev3_ms_played_bucket",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
    )
)

result = (
    with_details_listener
    .select("id", "ms_played_bucket_minus_max", "next_ms_played_bucket_minus_max")
    .unpivot(
        ids=["id"],
        values=["ms_played_bucket_minus_max", "next_ms_played_bucket_minus_max"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
)
