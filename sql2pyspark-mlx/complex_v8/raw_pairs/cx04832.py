from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_skipped_by_ms_played_fill = Window.partitionBy("is_skipped").orderBy(F.col("ms_played_filled_minus_median").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_or_unknown_track_id_b = Window.partitionBy("source_or_unknown", "track_id").orderBy("track_duration_sec", "id").rowsBetween(-2, Window.currentRow)
w_source_or_unknown_track_id_b_2 = Window.partitionBy("source_or_unknown", "track_id").orderBy("track_duration_sec", "id")

recent = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
    )
)

derived = (
    recent
    .select(
        "*",
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.lower(F.regexp_replace(F.trim(F.col("source")), " ", "_")).alias("source_slug"),
        F.abs(F.col("ms_played") - 223828).alias("ms_played_distance"),
    )
)

source_stats = (
    derived
    .groupBy("source")
    .agg(F.percentile_approx("ms_played_filled", 0.5).alias("median_ms_played_filled"))
)

vs_group = (
    derived.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.ms_played_filled") >= F.col("g.median_ms_played_filled"))
    .select(
        "s.*",
        "g.median_ms_played_filled",
        F.round(F.col("s.ms_played_filled") - F.col("g.median_ms_played_filled"), 2).alias("ms_played_filled_minus_median"),
    )
)

combined_details = (
    vs_group.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.listener_id"), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.ms_played",
        "s.is_skipped",
        "s.ms_played_filled",
        "s.source_or_unknown",
        "s.median_ms_played_filled",
        "s.ms_played_filled_minus_median",
        F.col("t.artist_id").alias("track_artist_id"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.last("ms_played_filled").over(w_is_skipped_by_ms_played_fill).alias("last_ms_played_filled"),
        F.max("track_duration_sec").over(w_source_or_unknown_track_id_b).alias("track_duration_sec_max_3"),
        F.round(F.cume_dist().over(w_source_or_unknown_track_id_b_2), 3).alias("track_duration_sec_cume_dist"),
    )
)

rollup_base = (
    ranked
    .filter(F.col("listener_id").isNotNull())
    .groupBy("track_artist_id", "source_or_unknown")
    .agg(
        F.sum("track_bpm").alias("total_track_bpm"),
        F.round(F.avg("listener_birth_year"), 2).alias("avg_listener_birth_year"),
    )
)

result = (
    rollup_base
    .groupBy("track_artist_id")
    .pivot("source_or_unknown", ["UNKNOWN", "playlist", "radio", "search"])
    .agg(F.avg("total_track_bpm"))
    .orderBy("track_artist_id")
)
