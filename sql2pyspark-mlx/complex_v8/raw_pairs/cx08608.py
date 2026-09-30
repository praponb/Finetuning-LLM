from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 365, "A").when(F.col("duration_sec") >= 183, "B").otherwise("C").alias("duration_sec_tier"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref_2"),
    )
)

artist_id_stats = (
    valid
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

outliers = (
    valid.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
    )
)

profiled = (
    outliers.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.min("bpm").alias("min_track_bpm"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.first_track_release_date",
        "t.min_track_bpm",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

combined_details_artist = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.duration_sec_tier",
        "s.title_ref_2",
        "s.median_bpm",
        "s.play_count",
        "s.min_track_bpm",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

summarized_rows_2 = (
    combined_details_artist.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

track_stats_3 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
        F.count("*").alias("track_count_2"),
        F.round(F.avg("duration_sec"), 2).alias("avg_track_duration_sec"),
    )
)

profiled_2 = (
    summarized_rows_2.alias("s")
    .join(track_stats_3.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
        "t.avg_track_duration_sec",
    )
)

result = (
    profiled_2
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_tier",
        "title_ref_2",
        "median_bpm",
        "play_count",
        "min_track_bpm",
        "artist_genre",
        "artist_debut_year",
        "track_count",
        "track_count_2",
        "avg_track_duration_sec",
        F.concat_ws(": ", F.col("title"), F.col("artist_genre")).alias("title_artist_genre_label"),
        F.concat_ws("|", F.col("title_ref_2"), F.col("title")).alias("title_ref_2_title_label"),
    )
    .orderBy(F.col("play_count").desc())
)
