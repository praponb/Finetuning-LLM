from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_listener_id = Window.partitionBy("listener_id")

filtered = (
    df_plays
    .filter((F.col("source") == "radio") & (F.col("listener_id").isNotNull()))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

qualified = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
)

with_details_listener = (
    qualified.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "inner")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.size(F.col("l.interests")) >= 2)
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.is_skipped",
        "s.source",
        "s.ms_played_filled",
        F.col("l.username").alias("listener_username"),
        F.col("l.interests").alias("listener_interests"),
        F.col("l.birth_year").alias("listener_birth_year"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.title").alias("track_title"),
    )
)

ranked = (
    with_details_listener
    .select(
        "*",
        F.min("listener_birth_year").over(w_all_by_ms_played_running).alias("min_listener_birth_year_so_far"),
        F.size(F.collect_set("device").over(w_listener_id)).alias("distinct_device_in_listener_id"),
        F.round(F.col("ms_played_filled") / F.nullif(F.sum("ms_played_filled").over(w_listener_id), F.lit(0)), 4).alias("ms_played_filled_share_of_listener_id"),
        F.min("listener_birth_year").over(w_listener_id).alias("listener_id_min_listener_birth_year"),
    )
)

summary = (
    ranked
    .filter(F.col("ms_played_filled").between(123500, 261750))
    .groupBy("track_id", "device", "listener_id")
    .agg(
        F.datediff(F.max("track_release_date"), F.min("track_release_date")).alias("track_release_date_span_days"),
        F.countDistinct("device").alias("distinct_devices"),
        F.array_sort(F.collect_set("listener_username")).alias("listener_username_list"),
        F.countDistinct("listener_id").alias("distinct_listener_ids"),
        F.countDistinct("track_id").alias("distinct_track_ids"),
        F.sum("ms_played_filled").alias("sum_ms_played_filled_check"),
    )
    .filter(F.col("sum_ms_played_filled_check") > 103750)
    .drop("sum_ms_played_filled_check")
)

enriched_track = (
    summary.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "inner")
    .filter(F.col("t.is_explicit"))
    .select(
        "s.track_id",
        "s.device",
        "s.listener_id",
        "s.track_release_date_span_days",
        "s.distinct_devices",
        "s.distinct_listener_ids",
        "s.distinct_track_ids",
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.artist_id").alias("track_artist_id"),
    )
)

result = (
    enriched_track
    .select(
        "track_id",
        "device",
        "listener_id",
        "distinct_devices",
        "distinct_listener_ids",
        "track_is_explicit",
        "track_duration_sec",
        "track_artist_id",
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
    .orderBy(F.col("distinct_devices").asc_nulls_last())
)
