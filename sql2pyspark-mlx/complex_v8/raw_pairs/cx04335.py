from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy(F.col("bpm").desc_nulls_first(), "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_bpm_moving_6 = Window.partitionBy("is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, Window.currentRow)
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_2 = Window.orderBy(F.col("bpm").asc_nulls_last(), "id")
w_is_explicit_artist_id_by_art = Window.partitionBy("is_explicit", "artist_id").orderBy("artist_debut_year", "id")

scoped = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "workout")) & (F.col("bpm").between(73, 180)))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
    )
)

combined_details_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.title_ref",
        "s.title_clean",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    combined_details_artist
    .select(
        "id",
        "artist_id",
        "title_ref",
        "artist_debut_year",
        F.row_number().over(w_all_by_bpm).alias("seq_no"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_bpm_full).alias("second_artist_debut_year"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.avg("artist_debut_year").over(w_is_explicit_by_bpm_moving_6), 2).alias("artist_debut_year_avg_7"),
        F.last("bpm").over(w_all_by_bpm_full).alias("last_bpm"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_id), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_id"),
        F.nth_value("artist_debut_year", 2).over(w_artist_id_by_bpm_full).alias("second_artist_debut_year_2"),
        F.lead("bpm", 2).over(w_all_by_bpm_2).alias("next2_bpm"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_is_explicit_artist_id_by_art)).alias("artist_debut_year_change"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "title_ref",
        "seq_no",
        "artist_id_max_bpm",
        "last_bpm",
        "artist_debut_year_share_of_artist_id",
        "second_artist_debut_year_2",
        "artist_debut_year_change",
    )
    .orderBy(
        F.col("second_artist_debut_year_2").asc_nulls_last(),
        F.col("seq_no").desc(),
        "id",
    )
    .limit(20)
)
