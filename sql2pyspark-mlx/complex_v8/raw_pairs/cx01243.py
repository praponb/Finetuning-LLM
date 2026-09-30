from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy("duration_sec", "id")
w_artist_country_artist_id = Window.partitionBy("artist_country", "artist_id")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year")
w_artist_country_by_bpm_prior = Window.partitionBy("artist_country").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_artist_country_by_bpm = Window.partitionBy("artist_country").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_artist_id_by_duration_sec_fu = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

joined = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.title_clean",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.sum("bpm").over(w_artist_id_by_artist_debut_ye).alias("bpm_sum_7"),
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_country_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_country_artist_id"),
        F.row_number().over(w_artist_id_by_duration_sec).alias("seq_no"),
        F.dense_rank().over(w_artist_id_by_artist_debut_ye_2).alias("artist_debut_year_dense_rank"),
        F.max("bpm").over(w_artist_country_by_bpm_prior).alias("bpm_max_2_prior"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_country_by_bpm)).alias("bpm_change"),
        F.round(F.percent_rank().over(w_artist_id_by_artist_debut_ye_2), 3).alias("artist_debut_year_percent_rank"),
        F.lead("bpm", 3).over(w_artist_id_by_duration_sec).alias("next3_bpm"),
        F.first("bpm").over(w_artist_id_by_duration_sec_fu).alias("first_bpm"),
    )
)

top_rows = with_trends.filter(F.col("artist_debut_year_dense_rank") <= 3)

result = (
    top_rows
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "title_clean",
        "artist_debut_year",
        "artist_country",
        "bpm_sum_7",
        "bpm_share_of_artist_country_artist_id",
        "artist_debut_year_dense_rank",
        "bpm_change",
        "artist_debut_year_percent_rank",
        "next3_bpm",
        "first_bpm",
        F.concat_ws("-", F.col("title"), F.col("title_clean")).alias("title_title_clean_label"),
    )
    .orderBy("artist_debut_year_percent_rank", "id")
    .limit(5)
)
