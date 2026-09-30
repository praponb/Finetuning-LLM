from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.month("release_date")).orderBy("duration_sec", "id")
w_is_explicit_release_date_mon_2 = Window.partitionBy("is_explicit", F.month("release_date")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 12)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

ranked = (
    scoped_tracks
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.dense_rank().over(w_is_explicit_release_date_mon).alias("duration_sec_dense_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_artist_id), 2).alias("duration_sec_vs_is_explicit_artist_id_avg"),
        F.row_number().over(w_is_explicit_release_date_mon).alias("rn"),
        F.last("bpm").over(w_is_explicit_release_date_mon_2).alias("last_bpm"),
    )
)

line_level_plays = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.bpm_share_of_is_explicit_artist_id",
        "s.duration_sec_dense_rank",
        "s.rn",
        "s.last_bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

joined_artist = (
    line_level_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        "s.bpm_share_of_is_explicit_artist_id",
        "s.duration_sec_dense_rank",
        "s.rn",
        "s.play_id",
        "s.play_is_skipped",
        "s.play_ms_played",
        "s.play_played_at",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.source").alias("play_source"),
    )
)

enriched = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "moods",
        "bpm_share_of_is_explicit_artist_id",
        "rn",
        "play_id",
        "play_ms_played",
        "play_played_at",
        "play_played_at_2",
        "play_source",
        F.length(F.trim(F.col("play_source"))).alias("play_source_len"),
        F.hour(F.col("play_played_at_2")).alias("play_played_at_2_hour"),
        F.date_format(F.col("play_played_at"), "MMM yyyy").alias("play_played_at_label"),
    )
)

result = (
    enriched
    .cube("artist_id", "play_id", "is_explicit")
    .agg(F.countDistinct("id").alias("distinct_ids"))
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("play_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
