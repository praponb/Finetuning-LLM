from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_n_is_explicit_true = Window.partitionBy("artist_id", "n_is_explicit_true").orderBy("first_release_date", "artist_id", "release_date_ym")
w_artist_is_verified_n_is_expl = Window.partitionBy("artist_is_verified", "n_is_explicit_true").orderBy("first_release_date", "artist_id", "release_date_ym")
w_artist_is_verified = Window.partitionBy("artist_is_verified")

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

stats = (
    scoped_tracks
    .groupBy(
        "artist_id",
        F.date_format("release_date", "yyyy-MM").alias("release_date_ym"),
    )
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.min("release_date").alias("first_release_date"),
        F.round(F.sum("bpm") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("bpm_to_duration_sec_ratio"),
        F.count(F.when(F.col("is_explicit"), 1)).alias("n_is_explicit_true"),
    )
)

with_activity = (
    stats.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("t.title"))).like("M%")) & (F.size(F.col("t.moods")) >= 2), "left_anti")
)

enriched_artist = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.artist_id",
        "s.release_date_ym",
        "s.first_release_date",
        "s.n_is_explicit_true",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

ranked = (
    enriched_artist
    .select(
        "*",
        F.dense_rank().over(w_artist_id_n_is_explicit_true).alias("first_release_date_dense_rank"),
        F.dense_rank().over(w_artist_is_verified_n_is_expl).alias("first_release_date_dense_rank_2"),
        F.max("n_is_explicit_true").over(w_artist_is_verified).alias("artist_is_verified_max_n_is_explicit_true"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.count("*").alias("track_count"))
)

profiled = (
    ranked.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_count"), F.lit(0)).alias("track_count"),
    )
)

with_parent = (
    profiled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

result = (
    with_parent
    .select(
        "artist_id",
        "release_date_ym",
        "n_is_explicit_true",
        "artist_debut_year",
        "artist_is_verified",
        "first_release_date_dense_rank",
        "first_release_date_dense_rank_2",
        "artist_is_verified_max_n_is_explicit_true",
        "artist_genre",
    )
    .orderBy(
        F.col("first_release_date_dense_rank").asc(),
        F.col("release_date_ym").desc(),
    )
)
