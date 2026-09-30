from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_artist_debut_year_ful = Window.orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_2 = Window.orderBy(F.col("duration_sec").desc(), "id")
w_duration_sec_rank_by_duratio = Window.partitionBy("duration_sec_rank").orderBy("duration_sec_change", "id").rowsBetween(-2, Window.currentRow)
w_duration_sec_rank_distinct_a = Window.partitionBy("duration_sec_rank", "distinct_artist_id_in_is_explicit").orderBy("bpm", "id").rowsBetween(-2, -1)
w_duration_sec_rank_distinct_a_2 = Window.partitionBy("duration_sec_rank", "distinct_artist_id_in_is_explicit").orderBy("bpm")
w_duration_sec_rank_is_explici = Window.partitionBy("duration_sec_rank", "is_explicit")
w_duration_sec_rank_distinct_a_3 = Window.partitionBy("duration_sec_rank", "distinct_artist_id_in_is_explicit").orderBy("bpm", "id")
w_all_by_second_artist_debut_y = Window.orderBy(F.col("second_artist_debut_year").asc_nulls_last())

scoped_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 76) & (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

with_details = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

metrics = (
    with_details
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.lag("artist_debut_year").over(w_all_by_duration_sec).alias("prev_artist_debut_year"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.last("artist_debut_year").over(w_all_by_artist_debut_year_ful).alias("last_artist_debut_year"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_all_by_bpm)).alias("duration_sec_change"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_bpm_full).alias("second_artist_debut_year"),
        F.last("artist_debut_year").over(w_all_by_duration_sec_full).alias("last_artist_debut_year_2"),
        F.lag("bpm", 3).over(w_all_by_duration_sec_2).alias("prev3_bpm"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

sequenced = (
    metrics
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "prev_artist_debut_year",
        "duration_sec_change",
        "distinct_artist_id_in_is_explicit",
        "second_artist_debut_year",
        "last_artist_debut_year_2",
        F.max("prev3_bpm").over(w_duration_sec_rank_by_duratio).alias("prev3_bpm_max_3"),
        F.round(F.avg("bpm").over(w_duration_sec_rank_distinct_a), 2).alias("bpm_avg_2_prior"),
        F.round(F.cume_dist().over(w_duration_sec_rank_distinct_a_2), 3).alias("bpm_cume_dist"),
        F.count(F.lit(1)).over(w_duration_sec_rank_is_explici).alias("rows_in_duration_sec_rank_is_explicit"),
        F.sum("duration_sec_rank").over(w_duration_sec_rank_is_explici).alias("duration_sec_rank_is_explicit_sum_duration_sec_r"),
        F.lead("last_artist_debut_year_2").over(w_duration_sec_rank_distinct_a_3).alias("next_last_artist_debut_year_2"),
        F.size(F.collect_set("artist_id").over(w_duration_sec_rank_is_explici)).alias("distinct_artist_id_in_duration_sec_rank_is_expli"),
        F.round(F.cume_dist().over(w_all_by_second_artist_debut_y), 3).alias("second_artist_debut_year_cume_dist"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "prev_artist_debut_year",
        "duration_sec_change",
        "distinct_artist_id_in_is_explicit",
        "second_artist_debut_year",
        "last_artist_debut_year_2",
        "prev3_bpm_max_3",
        "bpm_avg_2_prior",
        "bpm_cume_dist",
        "distinct_artist_id_in_duration_sec_rank_is_expli",
        "second_artist_debut_year_cume_dist",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
    .orderBy(F.col("id").desc(), F.col("duration_sec_change").desc_nulls_first())
)
