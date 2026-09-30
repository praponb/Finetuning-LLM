from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("E%")) | (F.col("bpm") < 163))
)

filtered = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre") == "Indie"), "left_anti")
)

with_children_plays = (
    filtered.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        "s.moods",
        F.col("p.id").alias("play_id"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
    )
)

play_device_stats = (
    with_children_plays
    .groupBy("play_device")
    .agg(F.avg("play_ms_played").alias("avg_play_ms_played"))
)

outliers = (
    with_children_plays.alias("s")
    .join(play_device_stats.alias("g"), F.col("g.play_device") == F.col("s.play_device"), "inner")
    .filter(F.col("s.play_ms_played") > F.col("g.avg_play_ms_played"))
    .select(
        "s.*",
        "g.avg_play_ms_played",
        F.round(F.col("s.play_ms_played") - F.col("g.avg_play_ms_played"), 2).alias("play_ms_played_minus_avg"),
    )
)

with_details = (
    outliers.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.play_id",
        "s.play_device",
        "s.play_listener_id",
        "s.play_ms_played_minus_avg",
        "a.artist_name",
        F.col("p.source").alias("play_source"),
    )
)

combined_details = (
    with_details.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.play_id",
        "s.play_device",
        "s.play_listener_id",
        "s.play_ms_played_minus_avg",
        "s.artist_name",
        "s.play_source",
        F.col("p.device").alias("play_device_2"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "artist_id",
        "moods",
        "play_device",
        "play_listener_id",
        "play_ms_played_minus_avg",
        "play_source",
        "play_device_2",
        F.concat(F.col("play_source"), F.lit("#"), F.col("id").cast("string")).alias("play_source_ref"),
    )
    .orderBy("play_source", F.col("id").asc_nulls_last())
)
