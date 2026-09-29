from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy("play_ms_played", "id", "play_id")

cleaned_tracks = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

line_level = (
    cleaned_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.source") == "search"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.release_date_label",
        "s.title_len",
        F.col("p.id").alias("play_id"),
        F.col("p.listener_id").alias("play_listener_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
    )
)

combined_details_listener = (
    line_level.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "left")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.play_id",
        "s.play_listener_id",
        "s.play_ms_played",
        "s.play_played_at",
        "s.play_source",
        F.col("l.interests").alias("listener_interests"),
        F.col("p.ms_played").alias("play_ms_played_2"),
    )
)

with_parent_play = (
    combined_details_listener.alias("s")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.play_id",
        "s.play_listener_id",
        "s.play_ms_played",
        "s.play_played_at",
        "s.listener_interests",
        F.col("p.track_id").alias("play_track_id"),
    )
)

deduped = (
    with_parent_play
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "play_ms_played",
        "play_played_at",
    )
    .orderBy(F.col("play_played_at").desc())
)
