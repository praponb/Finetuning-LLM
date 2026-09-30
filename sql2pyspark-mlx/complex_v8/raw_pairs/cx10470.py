from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_all_by_bpm_per_duration_sec = Window.orderBy(F.col("bpm_per_duration_sec").desc(), "id")
w_artist_id_by_artist_debut_ye = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").asc_nulls_last())
w_artist_id_by_bpm_per_duratio = Window.partitionBy("artist_id").orderBy(F.col("bpm_per_duration_sec").desc(), "id").rowsBetween(-3, -1)
w_artist_id_by_artist_debut_ye_2 = Window.partitionBy("artist_id").orderBy(F.col("artist_debut_year").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_per_duratio_2 = Window.partitionBy("artist_id").orderBy(F.col("bpm_per_duration_sec").desc())
w_artist_id_by_bpm_per_duratio_3 = Window.partitionBy("artist_id").orderBy(F.col("bpm_per_duration_sec").desc(), "id")
w_artist_id_by_bpm_per_duratio_4 = Window.partitionBy("artist_id").orderBy(F.col("bpm_per_duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tracks
    .filter(F.length(F.trim(F.col("title"))) >= 4)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
    )
)

enriched_artist = (
    scoped.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.lower(F.col("a.artist_name")).like("%pa%"))
    .select(
        "s.id",
        "s.artist_id",
        "s.bpm_per_duration_sec",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

with_trends = (
    enriched_artist
    .select(
        "id",
        "artist_id",
        "bpm_per_duration_sec",
        "artist_debut_year",
        F.round(F.cume_dist().over(w_all_by_bpm_per_duration_sec), 3).alias("bpm_per_duration_sec_cume_dist"),
        F.min("artist_debut_year").over(w_artist_id).alias("artist_id_min_artist_debut_year"),
        F.round(F.cume_dist().over(w_artist_id_by_artist_debut_ye), 3).alias("artist_debut_year_cume_dist"),
        F.round(F.avg("artist_debut_year").over(w_artist_id_by_bpm_per_duratio), 2).alias("artist_debut_year_avg_3_prior"),
        F.nth_value("bpm_per_duration_sec", 2).over(w_artist_id_by_artist_debut_ye_2).alias("second_bpm_per_duration_sec"),
        F.round(F.percent_rank().over(w_artist_id_by_bpm_per_duratio_2), 3).alias("bpm_per_duration_sec_percent_rank"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.lead("bpm_per_duration_sec", 1, 0).over(w_artist_id_by_bpm_per_duratio_3).alias("next_bpm_per_duration_sec"),
        F.nth_value("artist_debut_year", 2).over(w_artist_id_by_bpm_per_duratio_4).alias("second_artist_debut_year"),
        F.round(F.col("bpm_per_duration_sec") / F.nullif(F.sum("bpm_per_duration_sec").over(w_artist_id), F.lit(0)), 4).alias("bpm_per_duration_sec_share_of_artist_id"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "bpm_per_duration_sec",
        "artist_debut_year",
        "bpm_per_duration_sec_cume_dist",
        "artist_id_min_artist_debut_year",
        "artist_debut_year_cume_dist",
        "artist_debut_year_avg_3_prior",
        "bpm_per_duration_sec_percent_rank",
        "rows_in_artist_id",
        "next_bpm_per_duration_sec",
        "second_artist_debut_year",
        "bpm_per_duration_sec_share_of_artist_id",
        (F.col("bpm_per_duration_sec") - F.col("second_artist_debut_year")).alias("bpm_per_duration_sec_minus_second_artist_debut_y"),
    )
    .orderBy(F.col("second_artist_debut_year").desc())
)
