from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_is_explicit = Window.partitionBy("artist_country", "is_explicit")
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_country_is_explicit_b = Window.partitionBy("artist_country", "is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec_per_bpm", "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec_per_bpm", "id").rowsBetween(-1, -1)
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec_per_bpm", "id")
w_all_by_artist_debut_year_ful = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year_tie = Window.orderBy("artist_debut_year")

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.size(F.col("moods")).alias("moods_count"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

combined_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.duration_sec_per_bpm",
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
        F.col("a.country").alias("artist_country"),
    )
)

with_windows = (
    combined_details
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_artist_country_is_explicit)).alias("distinct_artist_id_in_artist_country_is_explicit"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit), 2).alias("bpm_vs_is_explicit_avg"),
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_artist_country_is_explicit), 2).alias("duration_sec_per_bpm_vs_artist_country_is_explic"),
        F.row_number().over(w_artist_country_is_explicit_b).alias("rn"),
        (F.col("duration_sec_per_bpm") - F.lag("duration_sec_per_bpm").over(w_is_explicit_by_duration_sec)).alias("duration_sec_per_bpm_change"),
        F.round(F.avg("bpm").over(w_is_explicit_by_duration_sec_2), 2).alias("bpm_avg_1_prior"),
        F.lead("duration_sec_per_bpm", 1, 0).over(w_all_by_artist_debut_year).alias("next_duration_sec_per_bpm"),
        F.row_number().over(w_is_explicit_artist_id_by_dur).alias("rn_2"),
        F.lead("bpm").over(w_all_by_artist_debut_year).alias("next_bpm"),
        F.round(F.avg("duration_sec_per_bpm").over(w_artist_country_is_explicit), 2).alias("artist_country_is_explicit_avg_duration_sec_per_"),
        F.sum("artist_debut_year").over(w_is_explicit).alias("is_explicit_sum_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_is_explicit), 2).alias("is_explicit_avg_artist_debut_year"),
        F.last("artist_debut_year").over(w_all_by_artist_debut_year_ful).alias("last_artist_debut_year"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_country_is_explicit), 2).alias("duration_sec_vs_artist_country_is_explicit_avg"),
        F.round(F.cume_dist().over(w_all_by_artist_debut_year_tie), 3).alias("artist_debut_year_cume_dist"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "artist_debut_year",
        "artist_name",
        "distinct_artist_id_in_artist_country_is_explicit",
        "bpm_vs_is_explicit_avg",
        "duration_sec_per_bpm_vs_artist_country_is_explic",
        "duration_sec_per_bpm_change",
        "bpm_avg_1_prior",
        "rn_2",
        "next_bpm",
        "artist_country_is_explicit_avg_duration_sec_per_",
        "is_explicit_sum_artist_debut_year",
        "is_explicit_avg_artist_debut_year",
        "last_artist_debut_year",
        "duration_sec_vs_artist_country_is_explicit_avg",
        "artist_debut_year_cume_dist",
        (F.col("last_artist_debut_year") + F.col("bpm_avg_1_prior")).alias("last_artist_debut_year_plus_bpm_avg_1_prior"),
        F.abs(F.col("artist_debut_year") - 2015).alias("artist_debut_year_distance"),
    )
    .orderBy("artist_id", F.col("title").desc())
)
