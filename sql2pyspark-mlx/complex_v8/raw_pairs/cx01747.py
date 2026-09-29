from pyspark.sql import functions as F

cleaned_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "moods",
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
    )
)

artist_id_stats = (
    cleaned_tracks
    .groupBy("artist_id")
    .agg(F.avg("bpm_filled").alias("avg_bpm_filled"))
)

vs_group = (
    cleaned_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_filled") > F.col("g.avg_bpm_filled"))
    .select(
        "s.*",
        "g.avg_bpm_filled",
        F.round(F.col("s.bpm_filled") - F.col("g.avg_bpm_filled"), 2).alias("bpm_filled_minus_avg"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    vs_group.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.count("*").alias("play_count_2"),
    )
)

with_counts = (
    summarized_rows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
    )
)

result = (
    with_counts
    .filter(F.length(F.trim(F.col("title"))) >= 5)
    .select(
        "id",
        "title",
        "moods",
        "bpm_filled",
        "bpm_filled_minus_avg",
        "play_count",
        "max_play_ms_played",
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)
