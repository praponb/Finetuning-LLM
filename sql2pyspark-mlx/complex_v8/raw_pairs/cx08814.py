from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(F.col("bpm") > 96)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
    )
)

derived = (
    filtered_tracks
    .filter(F.col("bpm") >= 82)
    .select(
        "*",
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

line_level_plays = (
    derived.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.moods",
        "s.bpm_minus_duration_sec",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

artist_id_stats = (
    line_level_plays
    .groupBy("artist_id")
    .agg(F.max("bpm_minus_duration_sec").alias("max_bpm_minus_duration_sec"))
)

compared = (
    line_level_plays.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_minus_duration_sec") == F.col("g.max_bpm_minus_duration_sec"))
    .select(
        "s.*",
        "g.max_bpm_minus_duration_sec",
        F.round(F.col("s.bpm_minus_duration_sec") - F.col("g.max_bpm_minus_duration_sec"), 2).alias("bpm_minus_duration_sec_minus_max"),
    )
)

joined = (
    compared.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.moods",
        "s.bpm_minus_duration_sec",
        "s.play_id",
        "s.play_played_at",
        "s.play_listener_id",
        "s.max_bpm_minus_duration_sec",
        "s.bpm_minus_duration_sec_minus_max",
        F.col("l.username").alias("listener_username"),
        F.col("l.birth_year").alias("listener_birth_year"),
    )
)

result = (
    joined
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "moods",
        "bpm_minus_duration_sec",
        "play_id",
        "play_played_at",
        "play_listener_id",
        "max_bpm_minus_duration_sec",
        "bpm_minus_duration_sec_minus_max",
        "listener_username",
        "listener_birth_year",
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("listener_username")), " ", "_")).alias("listener_username_slug"),
    )
    .orderBy(F.col("artist_id").asc(), F.col("bpm_minus_duration_sec").asc())
)
