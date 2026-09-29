from pyspark.sql import functions as F

prepared = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "source",
        F.when(F.col("device").isin("car", "tv"), "group_a").when(F.col("device") == "speaker", "group_b").otherwise("other").alias("device_group"),
    )
)

derived = (
    prepared
    .select(
        "id",
        "track_id",
        "ms_played",
        "device",
        "device_group",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.lower(F.trim(F.col("device_group"))).alias("device_group_clean"),
        F.date_trunc("hour", F.col("played_at")).alias("played_at_hour"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.min("ms_played").alias("min_play_ms_played"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.min_play_ms_played",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

combined_details = (
    summarized_rows.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.device_group",
        "s.played_at_label",
        "s.device_group_clean",
        "s.min_play_ms_played",
        F.col("t.duration_sec").alias("track_duration_sec"),
        F.col("t.title").alias("track_title"),
        F.col("t.bpm").alias("track_bpm"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

combined_details_track = (
    combined_details.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("t.artist_id"), "inner")
    .select(
        "s.id",
        "s.track_id",
        "s.ms_played",
        "s.device",
        "s.played_at_label",
        "s.device_group_clean",
        "s.min_play_ms_played",
        "s.track_duration_sec",
        "s.track_title",
        "s.track_bpm",
        "s.artist_debut_year",
        "s.artist_country",
        F.col("t.title").alias("track_title_2"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.country").alias("artist_country_2"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

summarized_rows_2 = (
    combined_details_track.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        "p.avg_play_ms_played",
        "p.total_play_ms_played",
    )
)

result = (
    summarized_rows_2
    .select(
        "id",
        "ms_played",
        "device",
        "played_at_label",
        "min_play_ms_played",
        "track_bpm",
        "artist_debut_year",
        "track_release_date",
        "track_moods",
        "first_played_at",
        "avg_play_ms_played",
        "total_play_ms_played",
        F.lower(F.trim(F.col("device"))).alias("device_clean"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("track_release_date")).alias("days_since_track_release_date"),
    )
    .orderBy(F.col("id").desc(), F.col("played_at_label").desc_nulls_first())
)
