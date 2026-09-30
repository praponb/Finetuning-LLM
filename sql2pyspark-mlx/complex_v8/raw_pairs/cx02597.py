from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("artist_id", "artist_genre").orderBy(F.col("last_track_release_date").desc(), F.col("id").desc())

filtered = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.abs(F.col("bpm") - 100).alias("bpm_distance"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count("*").alias("track_count"),
        F.max("release_date").alias("last_track_release_date"),
        F.count("*").alias("track_count_2"),
    )
)

with_stats = (
    filtered.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
        "t.last_track_release_date",
        F.coalesce(F.col("t.track_count_2"), F.lit(0)).alias("track_count_2"),
    )
)

with_details_artist = (
    with_stats.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_label",
        "s.bpm_distance",
        "s.track_count",
        "s.last_track_release_date",
        "s.track_count_2",
        F.col("a.genre").alias("artist_genre"),
    )
)

unique_rows = (
    with_details_artist
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children_plays = (
    unique_rows.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.track_count",
        "s.last_track_release_date",
        "s.track_count_2",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

derived = (
    with_children_plays
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "track_count",
        "play_id",
        "play_listener_id",
        "play_source",
        "play_ms_played",
        "play_played_at",
        F.abs(F.col("bpm") - 108).alias("bpm_distance"),
    )
)

with_details = (
    derived.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("p.listener_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.track_count",
        "s.play_id",
        "s.play_listener_id",
        "s.play_source",
        "s.play_ms_played",
        "s.play_played_at",
        "s.bpm_distance",
        F.col("p.track_id").alias("play_track_id"),
        F.col("p.listener_id").alias("play_listener_id_2"),
        F.col("l.plan").alias("listener_plan"),
    )
)

result = (
    with_details
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "track_count",
        "play_listener_id",
        "play_played_at",
        "listener_plan",
        F.date_format(F.col("play_played_at"), "yyyy-MM-dd").alias("play_played_at_label"),
    )
    .orderBy("id", F.col("play_listener_id").desc())
)
