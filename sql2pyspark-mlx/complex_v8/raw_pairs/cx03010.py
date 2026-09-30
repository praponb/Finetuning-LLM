from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("ms_played_tier", "track_id").orderBy("play_ms_played_minus_median", "id", "play_id")

cleaned = (
    df_plays
    .filter(F.col("listener_id").isNotNull())
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.length(F.trim(F.col("device"))).alias("device_len"),
        F.coalesce(F.col("source"), F.lit("UNKNOWN")).alias("source_or_unknown"),
        F.when(F.col("ms_played") >= 257789, "large").when(F.col("ms_played") >= 129336, "medium").otherwise("small").alias("ms_played_tier"),
    )
)

exploded_detail_plays = (
    cleaned.alias("s")
    .join(df_plays.alias("p"), F.col("p.listener_id") == F.col("s.listener_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.played_at",
        "s.is_skipped",
        "s.source",
        "s.device_len",
        "s.ms_played_tier",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

track_id_stats = (
    exploded_detail_plays
    .groupBy("track_id")
    .agg(F.percentile_approx("play_ms_played", 0.5).alias("median_play_ms_played"))
)

above_average = (
    exploded_detail_plays.alias("s")
    .join(track_id_stats.alias("g"), F.col("g.track_id") == F.col("s.track_id"), "inner")
    .filter(F.col("s.play_ms_played") >= F.col("g.median_play_ms_played"))
    .select(
        "s.*",
        "g.median_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.median_play_ms_played"), 2).alias("play_ms_played_minus_median"),
    )
)

joined = (
    above_average.alias("s")
    .join(df_listeners.alias("l"), (F.col("l.id") == F.col("s.listener_id")) & (F.col("l.birth_year") >= 1970), "left")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.play_id",
        "s.play_is_skipped",
        "s.median_play_ms_played",
        "s.play_ms_played_minus_median",
        F.col("l.country").alias("listener_country"),
    )
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_details_track = (
    deduped.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.col("t.duration_sec") <= 352), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.listener_id",
        "s.track_id",
        "s.is_skipped",
        "s.source",
        "s.ms_played_tier",
        "s.play_id",
        "s.play_is_skipped",
        "s.median_play_ms_played",
        "s.play_ms_played_minus_median",
        "s.listener_country",
        F.col("t.moods").alias("track_moods"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

with_parent_play = (
    with_details_track.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.*",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    with_parent_play
    .filter(F.col("source") == "search")
    .select(
        "id",
        "listener_id",
        "track_id",
        "is_skipped",
        "source",
        "play_id",
        "play_is_skipped",
        "median_play_ms_played",
        "listener_country",
        "track_moods",
        "track_is_explicit",
        "play_ms_played",
        "play_track_id",
        "play_source",
        F.coalesce(F.col("median_play_ms_played"), F.lit(0.0)).alias("median_play_ms_played_filled"),
        F.lower(F.regexp_replace(F.trim(F.col("play_source")), " ", "_")).alias("play_source_slug"),
    )
    .orderBy(F.col("play_id").asc_nulls_last())
)
