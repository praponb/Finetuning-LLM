from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.size(F.col("moods")) >= 2))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

with_parent = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.title_len",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country"),
    )
)

filtered = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isNotNull()) & (F.col("a.is_verified")), "left_semi")
)

enriched = (
    filtered
    .select(
        "*",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.when(F.col("artist_genre").isin("HipHop", "Classical"), "group_a").when(F.col("artist_genre") == "Indie", "group_b").otherwise("other").alias("artist_genre_group"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

summarized_rows = (
    enriched.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.max("bpm").alias("max_track_bpm"),
        F.round(F.avg("bpm"), 2).alias("avg_track_bpm"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count_2"),
    )
)

profiled = (
    summarized_rows.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        "t.max_track_bpm",
        "t.avg_track_bpm",
        F.coalesce(F.col("t.track_is_explicit_count_2"), F.lit(0)).alias("track_is_explicit_count_2"),
    )
)

detail_rows_plays = (
    profiled.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.title_len",
        "s.artist_debut_year",
        "s.artist_genre",
        "s.artist_genre_group",
        "s.max_track_bpm",
        "s.avg_track_bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.source").alias("play_source"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

result = (
    detail_rows_plays
    .groupingSets(
        [["play_is_skipped", "title_len"], ["play_is_skipped"], []],
        "play_is_skipped", "title_len",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("artist_debut_year").alias("min_artist_debut_year"),
        F.percentile_approx("max_track_bpm", 0.5).alias("median_max_track_bpm"),
        F.array_sort(F.collect_set("play_device")).alias("play_device_list"),
    )
    .orderBy(
        F.col("play_is_skipped").asc_nulls_last(),
        F.col("title_len").asc_nulls_last(),
    )
)
