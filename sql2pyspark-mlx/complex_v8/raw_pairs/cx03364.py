from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_all_by_release_date = Window.orderBy(F.col("release_date").desc(), "id")
w_artist_genre_artist_id = Window.partitionBy("artist_genre", "artist_id")
w_all_by_release_date_2 = Window.orderBy("release_date", "id")
w_has_chill_by_duration_sec = Window.partitionBy("has_chill").orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_prior_1 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)
w_release_date_year = Window.partitionBy(F.year("release_date"))

scoped_tracks = (
    df_tracks
    .filter(F.col("duration_sec") > 239)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.array_contains(F.col("moods"), "chill").alias("has_chill"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

with_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

metrics = (
    with_details_artist
    .select(
        "*",
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_genre), 2).alias("artist_debut_year_vs_artist_genre_avg"),
        F.row_number().over(w_all_by_release_date).alias("seq_no"),
        F.sum("bpm").over(w_artist_genre_artist_id).alias("artist_genre_artist_id_sum_bpm"),
        F.round(F.cume_dist().over(w_all_by_release_date_2), 3).alias("release_date_cume_dist"),
        F.round(F.avg("bpm_per_duration_sec").over(w_artist_genre_artist_id), 2).alias("artist_genre_artist_id_avg_bpm_per_duration_sec"),
        F.round(F.col("bpm_per_duration_sec") - F.avg("bpm_per_duration_sec").over(w_artist_genre_artist_id), 2).alias("bpm_per_duration_sec_vs_artist_genre_artist_id_a"),
        F.rank().over(w_has_chill_by_duration_sec).alias("duration_sec_rank"),
        F.nth_value("artist_debut_year", 2).over(w_artist_id_by_bpm_full).alias("second_artist_debut_year"),
        F.lead("duration_sec").over(w_all_by_release_date_2).alias("next_duration_sec"),
        F.sum("duration_sec").over(w_all_by_release_date_prior_1).alias("duration_sec_sum_1_prior"),
        F.max("bpm").over(w_release_date_year).alias("release_date_year_max_bpm"),
    )
)

result = (
    metrics
    .filter(F.col("next_duration_sec") >= 144)
    .select("id", "next_duration_sec", "artist_genre")
    .distinct()
    .orderBy("id", "artist_genre")
)
