from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_tracks = (
    df_tracks
    .filter((F.col("bpm") < 147) & (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

with_parent = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

with_details_artist = (
    with_parent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.upper(F.trim(F.col("a.artist_name"))).like("K%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.artist_debut_year",
        "a.artist_name",
    )
)

sequenced = (
    with_details_artist
    .select(
        "id",
        "artist_id",
        F.lag("artist_debut_year").over(w_artist_id_by_artist_debut_ye).alias("prev_artist_debut_year"),
        F.rank().over(w_artist_id_by_artist_debut_ye).alias("artist_debut_year_rank"),
        F.row_number().over(w_artist_id_by_artist_debut_ye).alias("artist_debut_year_seq"),
        F.lead("artist_debut_year", 1, 0).over(w_artist_id_by_artist_debut_ye).alias("next_artist_debut_year"),
        F.nth_value("artist_debut_year", 2).over(w_artist_id_by_artist_debut_ye_2).alias("second_artist_debut_year"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_artist_id_by_artist_debut_ye)).alias("artist_debut_year_change"),
    )
)

result = (
    sequenced
    .cube("artist_debut_year_seq", "artist_debut_year_rank")
    .agg(
        F.grouping("artist_debut_year_seq").alias("is_total_artist_debut_year_seq"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("artist_debut_year_seq").asc_nulls_last(),
        F.col("artist_debut_year_rank").asc_nulls_last(),
    )
)
