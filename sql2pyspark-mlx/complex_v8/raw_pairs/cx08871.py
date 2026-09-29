from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
    )
)

with_children_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.bpm",
        "s.is_explicit_label",
        "s.title_clean",
        "s.release_date_plus_3m",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
    )
)

derived = (
    with_children_plays
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "is_explicit_label",
        "title_clean",
        "release_date_plus_3m",
        "play_id",
        "play_listener_id",
        F.abs(F.col("bpm") - 108).alias("bpm_distance"),
    )
)

combined_details_listener = (
    derived.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.is_explicit_label",
        "s.play_id",
        "s.play_listener_id",
        F.col("l.interests").alias("listener_interests"),
        F.col("l.plan").alias("listener_plan"),
        F.col("l.username").alias("listener_username"),
        F.col("p.source").alias("play_source"),
    )
)

result = (
    combined_details_listener
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "play_id",
        "play_listener_id",
        "listener_plan",
        "listener_username",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)
