from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("artist_id").orderBy(F.col("duration_sec_minus_bpm").desc(), F.col("id").desc())

recent = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.col("release_date").isNotNull()))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.round(F.avg("duration_sec"), 2).alias("avg_track_duration_sec"))
)

profiled = (
    recent.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.avg_track_duration_sec")
)

latest = (
    profiled
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_stats = (
    latest.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.last_played_at")
)

with_details = (
    with_stats.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.duration_sec_minus_bpm",
        "s.title_prefix",
        "s.avg_track_duration_sec",
        "s.last_played_at",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

result = (
    with_details
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "duration_sec",
        "release_date",
        "duration_sec_minus_bpm",
        "title_prefix",
        "avg_track_duration_sec",
        "last_played_at",
        "artist_debut_year",
        "artist_is_verified",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.concat_ws(": ", F.col("artist_genre"), F.col("title_prefix")).alias("artist_genre_title_prefix_label"),
    )
    .orderBy(
        F.col("title_prefix").asc(),
        F.col("artist_genre_title_prefix_label").desc(),
    )
)
