from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm_ties = Window.orderBy(F.col("bpm").asc_nulls_last())
w_is_explicit_has_party = Window.partitionBy("is_explicit", "has_party")
w_all_by_bpm_ties_2 = Window.orderBy("bpm")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm = Window.orderBy(F.col("bpm").asc_nulls_last(), "id")
w_has_party_is_explicit_by_bpm = Window.partitionBy("has_party", "is_explicit").orderBy("bpm", "id").rowsBetween(-3, Window.currentRow)
w_has_party_is_explicit_by_bpm_2 = Window.partitionBy("has_party", "is_explicit").orderBy("bpm")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_bpm_2 = Window.orderBy(F.col("bpm").desc_nulls_first(), "id")
w_has_party_is_explicit_by_bpm_3 = Window.partitionBy("has_party", "is_explicit").orderBy("bpm", "id")
w_all_by_bpm_running_2 = Window.orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_tracks = (
    df_tracks
    .filter((F.col("duration_sec") <= 365) & (F.col("artist_id").isNotNull()))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.array_contains(F.col("moods"), "party").alias("has_party"),
    )
)

windowed = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "has_party",
        F.round(F.cume_dist().over(w_all_by_bpm_ties), 3).alias("bpm_cume_dist"),
        F.count(F.lit(1)).over(w_is_explicit_has_party).alias("rows_in_is_explicit_has_party"),
        F.round(F.percent_rank().over(w_all_by_bpm_ties_2), 3).alias("bpm_percent_rank"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank"),
        F.sum("bpm").over(w_all_by_bpm_running).alias("running_bpm"),
        F.lag("bpm", 3).over(w_all_by_bpm).alias("prev3_bpm"),
        F.sum("bpm").over(w_has_party_is_explicit_by_bpm).alias("bpm_sum_4"),
        F.round(F.percent_rank().over(w_has_party_is_explicit_by_bpm_2), 3).alias("bpm_percent_rank_2"),
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.lead("bpm").over(w_all_by_bpm_2).alias("next_bpm"),
        F.ntile(10).over(w_has_party_is_explicit_by_bpm_3).alias("bpm_decile"),
        F.sum("bpm").over(w_all_by_bpm_running_2).alias("running_bpm_2"),
        F.lag("bpm", 2).over(w_all_by_bpm_2).alias("prev2_bpm"),
    )
)

result = (
    windowed
    .filter(F.col("bpm_percent_rank") <= 0.8)
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "moods",
        "bpm_cume_dist",
        "rows_in_is_explicit_has_party",
        "bpm_percent_rank",
        "bpm_sum_4",
        "bpm_percent_rank_2",
        "next_bpm",
        "bpm_decile",
        "running_bpm_2",
        "prev2_bpm",
        F.array_contains(F.col("moods"), "sad").alias("has_sad"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy(
        F.col("running_bpm_2").asc(),
        F.col("bpm_decile").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
