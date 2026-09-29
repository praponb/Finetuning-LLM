from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))

prepared = (
    df_tracks
    .filter(F.col("artist_id") >= 6)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
    )
)

with_trends = (
    prepared
    .withColumn("release_date_dayofweek_vs_artist_id_release_date", F.round(F.col("release_date_dayofweek") - F.avg("release_date_dayofweek").over(w_artist_id_release_date_month), 2))
    .withColumn("release_date_dayofweek_share_of_artist_id_releas", F.round(F.col("release_date_dayofweek") / F.nullif(F.sum("release_date_dayofweek").over(w_artist_id_release_date_month), F.lit(0)), 4))
)

enriched_artist = (
    with_trends.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

line_level_plays = (
    enriched_artist.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") >= F.lit("2025-02-06 12:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.moods",
        "s.release_date_dayofweek",
        "s.release_date_dayofweek_share_of_artist_id_releas",
        "s.artist_name",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "release_date_dayofweek_share_of_artist_id_releas",
        "artist_name",
        "play_played_at",
        "play_ms_played",
        "play_is_skipped",
        "play_listener_id",
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
    )
    .orderBy(F.col("bpm").desc(), F.col("play_listener_id").desc())
)
