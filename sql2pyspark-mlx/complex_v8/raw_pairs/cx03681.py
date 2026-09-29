from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(F.col("bpm") > 76)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("device").isin("car", "phone"), 1)).alias("play_device_count"),
    )
)

with_counts = (
    recent.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.first_played_at",
        F.coalesce(F.col("p.play_device_count"), F.lit(0)).alias("play_device_count"),
    )
)

screened = (
    with_counts.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isin("Jazz", "Pop")), "left_semi")
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.round(F.avg("ms_played"), 2).alias("avg_play_ms_played"),
    )
)

with_counts_2 = (
    screened.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.avg_play_ms_played",
    )
)

artist_id_stats = (
    with_counts_2
    .groupBy("artist_id")
    .agg(F.avg("bpm").alias("avg_bpm"))
)

compared = (
    with_counts_2.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") > F.col("g.avg_bpm"))
    .select(
        "s.*",
        "g.avg_bpm",
        F.round(F.col("s.bpm") - F.col("g.avg_bpm"), 2).alias("bpm_minus_avg"),
    )
)

with_activity = (
    compared.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") <= 365) & (F.size(F.col("t.moods")) >= 1), "left_anti")
    .filter(F.col("duration_sec") <= 402)
)

result = (
    with_activity
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        "play_device_count",
        "play_count",
        "avg_play_ms_played",
        "bpm_minus_avg",
        (F.floor(F.col("bpm_minus_avg") / 50) * 50).alias("bpm_minus_avg_bucket"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
    .orderBy(F.col("bpm_minus_avg_bucket").desc(), "title")
)
