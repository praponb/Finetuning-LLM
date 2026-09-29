from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("G%"))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
        F.sum("ms_played").alias("total_play_ms_played"),
    )
)

summarized_rows = (
    valid_tracks.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.total_play_ms_played",
    )
)

enriched = (
    summarized_rows.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.moods",
        "s.last_played_at",
        "s.play_count",
        "s.total_play_ms_played",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isNotNull()) & (F.col("p.played_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left_semi")
)

artist_id_stats = (
    screened
    .groupBy("artist_id")
    .agg(F.avg("total_play_ms_played").alias("avg_total_play_ms_played"))
)

outliers = (
    screened.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.total_play_ms_played") > F.col("g.avg_total_play_ms_played"))
    .select(
        "s.*",
        "g.avg_total_play_ms_played",
        F.round(F.col("s.total_play_ms_played") - F.col("g.avg_total_play_ms_played"), 2).alias("total_play_ms_played_minus_avg"),
    )
)

enriched_2 = (
    outliers.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year_2"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    enriched_2
    .groupBy("artist_id", "artist_genre")
    .pivot("artist_country", ["JP", "BR", "KR"])
    .agg(F.min("avg_total_play_ms_played").alias("min_v"), F.sum("avg_total_play_ms_played").alias("sum_v"))
)
