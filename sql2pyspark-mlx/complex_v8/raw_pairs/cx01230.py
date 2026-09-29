from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_country_genre_by_debut_year = Window.partitionBy("country", "genre").orderBy("debut_year_filled")
w_country_genre_by_debut_year_2 = Window.partitionBy("country", "genre").orderBy("debut_year_filled", "id")
w_is_verified = Window.partitionBy("is_verified")

base_artists = (
    df_artists
    .filter(~F.col("genre").isin("Classical", "Indie"))
    .select(
        "id",
        "artist_name",
        "genre",
        "country",
        "debut_year",
        "is_verified",
        F.coalesce(F.col("debut_year"), F.lit(0)).alias("debut_year_filled"),
        F.substring(F.trim(F.col("artist_name")), 1, 2).alias("artist_name_prefix"),
    )
)

without_matches = (
    base_artists
    .crossJoin(base_artists.agg(F.min("debut_year").alias("overall_min_debut_year")))
    .filter(F.col("debut_year") > F.col("overall_min_debut_year"))
    .drop("overall_min_debut_year")
)

screened = (
    without_matches.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.id")) & (F.col("t.is_explicit")), "left_semi")
    .filter(F.col("debut_year") <= 2018)
)

sequenced = (
    screened
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_filled",
        F.dense_rank().over(w_country_genre_by_debut_year).alias("debut_year_filled_dense_rank"),
        F.row_number().over(w_country_genre_by_debut_year_2).alias("seq_no"),
        F.lag("debut_year_filled", 3).over(w_country_genre_by_debut_year_2).alias("prev3_debut_year_filled"),
        F.round(F.col("debut_year") - F.avg("debut_year").over(w_is_verified), 2).alias("debut_year_vs_is_verified_avg"),
    )
)

result = (
    sequenced
    .filter(F.upper(F.trim(F.col("artist_name"))).like("P%"))
    .select(
        "id",
        "artist_name",
        "country",
        "debut_year",
        "is_verified",
        "debut_year_filled",
        "debut_year_filled_dense_rank",
        "seq_no",
        "prev3_debut_year_filled",
        "debut_year_vs_is_verified_avg",
        (F.col("prev3_debut_year_filled") - F.col("seq_no")).alias("prev3_debut_year_filled_minus_seq_no"),
    )
)
