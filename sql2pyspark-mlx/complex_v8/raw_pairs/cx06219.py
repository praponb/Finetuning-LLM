from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_artist_id_by_bpm_moving_3 = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id")
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id = Window.partitionBy("artist_id")

scoped_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 76)
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label_2"),
    )
)

enriched = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.release_date_label",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "artist_debut_year",
        F.count(F.lit(1)).over(w_artist_id_release_date_month).alias("rows_in_artist_id_release_date_month"),
        F.round(F.avg("duration_sec").over(w_artist_id_by_bpm_moving_3), 2).alias("duration_sec_avg_4"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_release_date)).alias("bpm_change"),
        F.min("duration_sec").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_min_duration_sec"),
        F.round(F.percent_rank().over(w_artist_id_by_artist_debut_ye), 3).alias("artist_debut_year_percent_rank"),
        F.first("release_date").over(w_artist_id_by_artist_debut_ye_2).alias("first_release_date"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.lead("release_date").over(w_artist_id_by_bpm).alias("next_release_date"),
        F.min("artist_debut_year").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_min_artist_debut_ye"),
        F.first("artist_debut_year").over(w_artist_id_by_artist_debut_ye_2).alias("first_artist_debut_year"),
        F.ntile(5).over(w_artist_id_by_bpm).alias("bpm_quintile"),
    )
)

derived = (
    windowed
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_debut_year",
        "rows_in_artist_id_release_date_month",
        "duration_sec_avg_4",
        "bpm_change",
        "artist_id_release_date_month_min_duration_sec",
        "artist_debut_year_percent_rank",
        "first_release_date",
        "next_release_date",
        "artist_id_release_date_month_min_artist_debut_ye",
        F.month(F.col("first_release_date")).alias("first_release_date_month"),
    )
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "release_date",
        "artist_debut_year",
        "rows_in_artist_id_release_date_month",
        "duration_sec_avg_4",
        "artist_id_release_date_month_min_duration_sec",
        "first_release_date",
        "next_release_date",
        "first_release_date_month",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
