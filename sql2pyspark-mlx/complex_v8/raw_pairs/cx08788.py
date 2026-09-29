from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_genre = Window.partitionBy("is_explicit", "artist_genre")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc(), "id")
w_artist_genre_by_release_date_2 = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_by_duratio = Window.partitionBy(F.year("release_date")).orderBy("duration_sec", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_genre_by_re = Window.partitionBy("artist_id", "artist_genre").orderBy("release_date", "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_artist_genre_by_re_2 = Window.partitionBy("artist_id", "artist_genre").orderBy("release_date")

base_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-06-03").cast("date"), F.lit("2025-08-28").cast("date"))) & (F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
    )
)

combined_details_artist = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.genre").isin("Jazz", "Indie")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

ranked = (
    combined_details_artist
    .select(
        "id",
        "duration_sec",
        "release_date",
        "artist_genre",
        "artist_debut_year",
        F.size(F.collect_set("artist_genre").over(w_artist_id)).alias("distinct_artist_genre_in_artist_id"),
        F.count(F.lit(1)).over(w_is_explicit_artist_genre).alias("rows_in_is_explicit_artist_genre"),
        F.rank().over(w_artist_genre_by_release_date).alias("release_date_rank"),
        F.last("artist_debut_year", ignorenulls=True).over(w_artist_genre_by_release_date_2).alias("last_known_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_is_explicit_artist_genre), 2).alias("artist_debut_year_vs_is_explicit_artist_genre_av"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_id"),
        F.row_number().over(w_release_date_year_by_duratio).alias("rn"),
        F.min("duration_sec").over(w_is_explicit_by_duration_sec).alias("min_duration_sec_so_far"),
        F.lag("artist_debut_year").over(w_artist_id_artist_genre_by_re).alias("prev_artist_debut_year"),
        F.nth_value("artist_debut_year", 2).over(w_is_explicit_by_duration_sec_2).alias("second_artist_debut_year"),
        F.rank().over(w_artist_id_artist_genre_by_re_2).alias("release_date_rank_2"),
    )
)

first_rows = (
    ranked
    .filter((F.col("rn") == 1) & (F.col("release_date") >= F.lit("2024-09-26").cast("date")))
)

result = (
    first_rows
    .filter(F.col("last_known_artist_debut_year") <= 2016)
    .select(
        "id",
        "duration_sec",
        "release_date",
        "artist_genre",
        "artist_debut_year",
        "distinct_artist_genre_in_artist_id",
        "release_date_rank",
        "artist_debut_year_vs_is_explicit_artist_genre_av",
        "min_duration_sec_so_far",
        "second_artist_debut_year",
        "release_date_rank_2",
        F.upper(F.trim(F.col("artist_genre"))).alias("artist_genre_clean"),
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
    .orderBy(
        F.col("release_date_rank_2").asc_nulls_last(),
        F.col("artist_debut_year").desc(),
    )
)
