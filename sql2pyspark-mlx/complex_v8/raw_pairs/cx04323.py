from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_artist_genre_artist_id_by_re = Window.partitionBy("artist_genre", "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_genre_by_du = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_artist_genre_by_du_2 = Window.partitionBy("artist_id", "artist_genre").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_artist_id_by_re_2 = Window.partitionBy("artist_genre", "artist_id").orderBy("release_date", "id")
w_artist_genre_artist_id_by_re_3 = Window.partitionBy("artist_genre", "artist_id").orderBy("release_date")
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc())
w_artist_id_artist_genre = Window.partitionBy("artist_id", "artist_genre")

base_tracks = (
    df_tracks
    .filter(F.col("release_date") >= F.lit("2024-08-26").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

joined = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.moods",
        "s.bpm_bucket",
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.lead("release_date", 2).over(w_all_by_duration_sec).alias("next2_release_date"),
        F.lead("duration_sec", 1, 0).over(w_all_by_duration_sec).alias("next_duration_sec"),
        F.min("duration_sec").over(w_artist_genre_artist_id_by_re).alias("min_duration_sec_so_far"),
        F.round(F.avg("duration_sec").over(w_artist_id_artist_genre_by_du), 2).alias("duration_sec_avg_7"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_artist_genre_by_du_2).alias("first_known_release_date"),
        F.round(F.percent_rank().over(w_artist_genre_artist_id_by_re_2), 3).alias("release_date_percent_rank"),
        F.dense_rank().over(w_artist_genre_artist_id_by_re_3).alias("release_date_dense_rank"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.count(F.lit(1)).over(w_artist_id_artist_genre).alias("rows_in_artist_id_artist_genre"),
    )
)

leaders = (
    with_trends
    .filter(F.col("duration_sec_rank") <= 5)
    .drop("duration_sec_rank")
)

result = (
    leaders
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        "artist_genre",
        "artist_name",
        "next2_release_date",
        "next_duration_sec",
        "min_duration_sec_so_far",
        "first_known_release_date",
        "release_date_percent_rank",
        "release_date_dense_rank",
        F.round(F.col("min_duration_sec_so_far") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("min_duration_sec_so_far_per_duration_sec"),
        F.upper(F.trim(F.col("artist_genre"))).alias("artist_genre_clean"),
    )
)
