from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc())
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_all_by_duration_sec_share_of = Window.orderBy("duration_sec_share_of_artist_id", "id")
w_bpm_tercile_by_duration_sec = Window.partitionBy("bpm_tercile").orderBy(F.col("duration_sec_share_of_artist_id").desc())
w_bpm_tercile_artist_id = Window.partitionBy("bpm_tercile", "artist_id")

scoped_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ig%")) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.length(F.trim(F.col("a.artist_name"))) >= 7)
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        "artist_name",
        "artist_is_verified",
        F.sum("bpm").over(w_artist_id_by_bpm_running).alias("running_bpm"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.nth_value("bpm", 2).over(w_artist_id_by_bpm_full).alias("second_bpm"),
        F.round(F.cume_dist().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.sum("artist_debut_year").over(w_artist_id).alias("artist_id_sum_artist_debut_year"),
        F.dense_rank().over(w_artist_id_by_bpm_ties).alias("bpm_dense_rank"),
        F.ntile(3).over(w_artist_id_by_bpm).alias("bpm_tercile"),
    )
)

with_windows = (
    with_trends
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_id_artist_is_verified).alias("rows_in_artist_id_artist_is_verified"),
        F.dense_rank().over(w_all_by_duration_sec_share_of).alias("duration_sec_share_of_artist_id_dense_rank"),
        F.round(F.cume_dist().over(w_bpm_tercile_by_duration_sec), 3).alias("duration_sec_share_of_artist_id_cume_dist"),
        F.max("bpm_dense_rank").over(w_bpm_tercile_artist_id).alias("bpm_tercile_artist_id_max_bpm_dense_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "title",
        "bpm",
        "moods",
        "artist_name",
        "running_bpm",
        "duration_sec_share_of_artist_id",
        "second_bpm",
        "bpm_cume_dist",
        "artist_id_sum_artist_debut_year",
        "bpm_dense_rank",
        "bpm_tercile",
        "duration_sec_share_of_artist_id_dense_rank",
        "duration_sec_share_of_artist_id_cume_dist",
        "bpm_tercile_artist_id_max_bpm_dense_rank",
        (F.floor(F.col("running_bpm") / 250) * 250).alias("running_bpm_bucket"),
        F.when(F.col("running_bpm") >= 1104, "A").when(F.col("running_bpm") >= 669, "B").otherwise("C").alias("running_bpm_tier"),
    )
    .orderBy(
        "duration_sec_share_of_artist_id_dense_rank",
        F.col("bpm_tercile").desc(),
    )
)
