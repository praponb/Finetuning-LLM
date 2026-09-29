from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_is_explicit_labe = Window.partitionBy("is_explicit", "is_explicit_label").orderBy(F.col("duration_sec_filled").asc_nulls_last())
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_label_by_bpm = Window.partitionBy("is_explicit_label").orderBy("bpm", "id")
w_is_explicit_label_by_duratio = Window.partitionBy("is_explicit_label").orderBy("duration_sec", "id")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm = Window.orderBy("bpm", "id")
w_is_explicit_is_explicit_labe_2 = Window.partitionBy("is_explicit", "is_explicit_label").orderBy(F.col("duration_sec_filled").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_label_by_bpm_ful = Window.partitionBy("is_explicit_label").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit_by_bpm_2 = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_tracks
    .filter((F.array_contains(F.col("moods"), "focus")) & (F.lower(F.col("title")).like("%ts%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

ranked = (
    valid
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        F.round(F.cume_dist().over(w_is_explicit_is_explicit_labe), 3).alias("duration_sec_filled_cume_dist"),
        F.round(F.col("duration_sec_filled") / F.nullif(F.sum("duration_sec_filled").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_filled_share_of_artist_id"),
        F.lead("duration_sec").over(w_artist_id_is_explicit_by_bpm).alias("next_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
        (F.col("duration_sec_filled") - F.lag("duration_sec_filled").over(w_is_explicit_label_by_bpm)).alias("duration_sec_filled_change"),
        F.round(F.percent_rank().over(w_is_explicit_label_by_duratio), 3).alias("duration_sec_percent_rank"),
        F.first("bpm").over(w_all_by_bpm_full).alias("first_bpm"),
        F.lead("duration_sec", 3).over(w_all_by_bpm).alias("next3_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("artist_id_is_explicit_avg_bpm"),
        F.last("duration_sec_filled").over(w_is_explicit_is_explicit_labe_2).alias("last_duration_sec_filled"),
        F.nth_value("duration_sec_filled", 2).over(w_is_explicit_label_by_bpm_ful).alias("second_duration_sec_filled"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
        F.sum("duration_sec_filled").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_duration_sec_filled"),
        F.sum("duration_sec").over(w_artist_id_is_explicit_by_bpm_2).alias("duration_sec_sum_3"),
    )
)

derived = (
    ranked
    .filter(F.col("duration_sec_sum_3") < 734)
    .select(
        "*",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
        (F.col("duration_sec") + F.col("last_duration_sec_filled")).alias("duration_sec_plus_last_duration_sec_filled"),
    )
)

result = (
    derived
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
        "duration_sec_filled_cume_dist",
        "duration_sec_filled_share_of_artist_id",
        "next_duration_sec",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "duration_sec_filled_change",
        "next3_duration_sec",
        "rows_in_artist_id",
        "last_duration_sec_filled",
        "second_duration_sec_filled",
        "duration_sec_vs_artist_id_avg",
        "artist_id_is_explicit_sum_duration_sec_filled",
        "duration_sec_sum_3",
        "is_explicit_flag",
        "duration_sec_plus_last_duration_sec_filled",
    )
    .orderBy("duration_sec_filled_share_of_artist_id")
)
