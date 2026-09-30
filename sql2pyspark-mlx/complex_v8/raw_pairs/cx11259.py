from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_artist_country = Window.partitionBy("artist_id", "artist_country")
w_all_by_artist_debut_year = Window.orderBy(F.col("artist_debut_year").desc(), "id")
w_artist_country_by_artist_deb = Window.partitionBy("artist_country").orderBy(F.col("artist_debut_year").desc(), "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc_nulls_first(), "id")
w_all_by_artist_debut_year_pri = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(-2, -1)
w_artist_id_artist_country_by = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_all_by_artist_debut_year_2 = Window.orderBy(F.col("artist_debut_year").desc_nulls_first(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_artist_country_by_2 = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_country_by_3 = Window.partitionBy("artist_id", "artist_country").orderBy(F.col("artist_debut_year").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tracks = (
    df_tracks
    .filter(F.col("duration_sec") < 312)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
    )
)

with_parent = (
    cleaned_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        F.col("a.country").alias("artist_country"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

windowed = (
    with_parent
    .select(
        "*",
        F.max("bpm").over(w_artist_id_artist_country).alias("artist_id_artist_country_max_bpm"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_all_by_artist_debut_year)).alias("artist_debut_year_change"),
        F.ntile(4).over(w_artist_country_by_artist_deb).alias("artist_debut_year_quartile"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_artist_id_by_bpm)).alias("artist_debut_year_change_2"),
        F.round(F.avg("artist_debut_year").over(w_all_by_artist_debut_year_pri), 2).alias("artist_debut_year_avg_2_prior"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_artist_country_by), 2).alias("artist_debut_year_avg_2_prior_2"),
        F.ntile(10).over(w_all_by_artist_debut_year_2).alias("artist_debut_year_decile"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.min("bpm").over(w_artist_id_artist_country_by_2).alias("min_bpm_so_far"),
        F.nth_value("bpm", 2).over(w_artist_id_artist_country_by_3).alias("second_bpm"),
    )
)

enriched = (
    windowed
    .select(
        "*",
        (F.floor(F.col("artist_debut_year_change") / 1000) * 1000).alias("artist_debut_year_change_bucket"),
        F.coalesce(F.col("artist_country"), F.lit("UNKNOWN")).alias("artist_country_or_unknown"),
        F.abs(F.col("artist_debut_year_change") - -809).alias("artist_debut_year_change_distance"),
    )
)

result = (
    enriched
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "artist_debut_year",
        "artist_id_artist_country_max_bpm",
        "artist_debut_year_change",
        "artist_debut_year_quartile",
        "artist_debut_year_avg_2_prior",
        "artist_debut_year_avg_2_prior_2",
        "artist_debut_year_decile",
        "artist_id_max_bpm",
        "second_bpm",
        "artist_debut_year_change_bucket",
        "artist_debut_year_change_distance",
    )
    .orderBy("title")
)
