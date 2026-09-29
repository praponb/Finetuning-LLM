from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ms_played_tier_by_last_playe = Window.partitionBy("ms_played_tier").orderBy(F.col("last_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_is_explicit_device_by = Window.partitionBy("track_is_explicit", "device").orderBy(F.col("ms_played_bucket").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_ms_played_tier = Window.partitionBy("ms_played_tier")
w_source_device_by_last_played = Window.partitionBy("source", "device").orderBy(F.col("last_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_source_device_by_last_played_2 = Window.partitionBy("source", "device").orderBy(F.col("last_played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_rows_track_id_by_n_rows_ru = Window.partitionBy("n_rows", "track_id").orderBy("n_rows", "is_skipped_flag", "track_id", "ms_played_tier").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_n_rows_track_id_by_n_rows = Window.partitionBy("n_rows", "track_id").orderBy("n_rows", "is_skipped_flag", "track_id", "ms_played_tier")

prepared = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("source").isin("radio", "playlist")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        (F.floor(F.col("ms_played") / 50000) * 50000).alias("ms_played_bucket"),
        F.when(F.col("is_skipped"), 1).otherwise(0).alias("is_skipped_flag"),
        F.when(F.col("ms_played") >= 290754, "A").when(F.col("ms_played") >= 95960, "B").otherwise("C").alias("ms_played_tier"),
    )
)

combined_details = (
    prepared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_bucket",
        "s.is_skipped_flag",
        "s.ms_played_tier",
        F.col("l.plan").alias("listener_plan"),
        F.col("l.interests").alias("listener_interests"),
        F.col("t.is_explicit").alias("track_is_explicit"),
    )
)

play_stats = (
    df_plays
    .groupBy("listener_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    combined_details.alias("s")
    .join(play_stats.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "left")
    .select("s.*", "p.last_played_at")
)

source_stats = (
    with_stats
    .groupBy("source")
    .agg(F.avg("ms_played_bucket").alias("avg_ms_played_bucket"))
)

compared = (
    with_stats.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played_bucket") > F.col("g.avg_ms_played_bucket"))
    .select(
        "s.*",
        "g.avg_ms_played_bucket",
        F.round(F.col("s.ms_played_bucket") - F.col("g.avg_ms_played_bucket"), 2).alias("ms_played_bucket_minus_avg"),
    )
)

sequenced = (
    compared
    .select(
        "id",
        "track_id",
        "device",
        "source",
        "ms_played_bucket",
        "is_skipped_flag",
        "ms_played_tier",
        "listener_plan",
        "listener_interests",
        "track_is_explicit",
        "last_played_at",
        "avg_ms_played_bucket",
        "ms_played_bucket_minus_avg",
        F.last("ms_played", ignorenulls=True).over(w_ms_played_tier_by_last_playe).alias("last_known_ms_played"),
        F.max("avg_ms_played_bucket").over(w_track_is_explicit_device_by).alias("avg_ms_played_bucket_max_2_prior"),
        F.min("ms_played_bucket_minus_avg").over(w_ms_played_tier).alias("ms_played_tier_min_ms_played_bucket_minus_avg"),
        F.count(F.lit(1)).over(w_source_device_by_last_played).alias("running_count"),
        F.last("avg_ms_played_bucket").over(w_source_device_by_last_played_2).alias("last_avg_ms_played_bucket"),
    )
)

stats = (
    sequenced
    .groupBy("is_skipped_flag", "track_id", "ms_played_tier")
    .agg(
        F.count(F.when(F.col("track_is_explicit"), 1)).alias("n_track_is_explicit_true"),
        F.array_sort(F.collect_set("source")).alias("source_list"),
        F.round(100 * F.count(F.when(F.col("listener_plan") == "free", 1)) / F.count("*"), 1).alias("pct_listener_plan_free"),
        F.count("*").alias("n_rows"),
    )
)

with_windows = (
    stats
    .select(
        "*",
        F.count(F.lit(1)).over(w_ms_played_tier).alias("rows_in_ms_played_tier"),
        F.first("is_skipped_flag", ignorenulls=True).over(w_n_rows_track_id_by_n_rows_ru).alias("first_known_is_skipped_flag"),
        F.lag("pct_listener_plan_free").over(w_n_rows_track_id_by_n_rows).alias("prev_pct_listener_plan_free"),
        F.max("pct_listener_plan_free").over(w_ms_played_tier).alias("ms_played_tier_max_pct_listener_plan_free"),
    )
)

result = (
    with_windows
    .select(
        "is_skipped_flag",
        "track_id",
        "n_track_is_explicit_true",
        "source_list",
        "pct_listener_plan_free",
    )
    .orderBy("n_track_is_explicit_true")
)
