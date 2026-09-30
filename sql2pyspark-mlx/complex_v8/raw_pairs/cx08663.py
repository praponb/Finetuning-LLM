from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date_we = Window.partitionBy("artist_id").orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_release_date_week = Window.partitionBy("artist_id", F.year("release_date_week")).orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").desc(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_all_by_bpm_ties = Window.orderBy("bpm")
w_artist_id_by_release_date_we_2 = Window.partitionBy("artist_id").orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        (F.col("duration_sec") + F.col("bpm")).alias("duration_sec_plus_bpm"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

with_parent_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", "a.artist_name", F.col("a.debut_year").alias("artist_debut_year"))
)

metrics = (
    with_parent_artist
    .select(
        "*",
        F.last("artist_debut_year").over(w_artist_id_by_release_date_we).alias("last_artist_debut_year"),
        F.count(F.lit(1)).over(w_artist_id_release_date_week).alias("running_count"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_artist_debut_ye)).alias("bpm_change"),
        F.row_number().over(w_artist_id_by_artist_debut_ye).alias("seq_no"),
        F.lag("release_date_week").over(w_artist_id_by_bpm).alias("prev_release_date_week"),
        F.dense_rank().over(w_all_by_bpm_ties).alias("bpm_dense_rank"),
        F.min("duration_sec_plus_bpm").over(w_artist_id).alias("artist_id_min_duration_sec_plus_bpm"),
        F.count(F.lit(1)).over(w_artist_id_by_release_date_we_2).alias("running_count_2"),
    )
)

result = (
    metrics
    .select(
        "id",
        "title",
        "moods",
        "duration_sec_plus_bpm",
        "artist_name",
        "artist_debut_year",
        "last_artist_debut_year",
        "prev_release_date_week",
        "bpm_dense_rank",
        "running_count_2",
        (F.col("bpm_change") - F.col("bpm")).alias("bpm_change_minus_bpm"),
    )
    .orderBy(
        "last_artist_debut_year",
        F.col("bpm_change_minus_bpm").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
