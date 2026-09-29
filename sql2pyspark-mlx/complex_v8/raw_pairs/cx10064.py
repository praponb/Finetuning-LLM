from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("bpm").desc())
w_artist_is_verified_by_releas = Window.partitionBy("artist_is_verified").orderBy(F.col("release_date").desc(), "id")
w_artist_id_artist_is_verified_2 = Window.partitionBy("artist_id", "artist_is_verified").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("R%"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
    )
)

with_parent_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.country").isNotNull()), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.title_len",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_windows = (
    with_parent_artist
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.dense_rank().over(w_artist_id_artist_is_verified).alias("bpm_dense_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.max("duration_sec").over(w_artist_id).alias("artist_id_max_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        (F.col("title_len") - F.lag("title_len").over(w_artist_is_verified_by_releas)).alias("title_len_change"),
        F.first("artist_debut_year").over(w_artist_id_artist_is_verified_2).alias("first_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_artist_id), 2).alias("artist_id_avg_artist_debut_year"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "title_len",
        "artist_is_verified",
        "bpm_dense_rank",
        "bpm_share_of_artist_id",
        "artist_id_max_duration_sec",
        "rows_in_artist_id",
        "first_artist_debut_year",
        "artist_id_avg_artist_debut_year",
    )
    .orderBy("release_date", F.col("title_len").desc())
)
