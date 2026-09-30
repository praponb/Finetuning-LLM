from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_per_bpm = Window.orderBy("duration_sec_per_bpm")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id")
w_artist_id_by_duration_sec_mo = Window.partitionBy("artist_id").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_is_explicit_artist_id_by_dur_3 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_per_bpm_2 = Window.orderBy("duration_sec_per_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_ties = Window.partitionBy("is_explicit").orderBy("bpm")

filtered_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
    )
)

with_trends = (
    filtered_tracks
    .select(
        "*",
        F.round(F.percent_rank().over(w_artist_id_by_duration_sec), 3).alias("duration_sec_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_per_bpm), 3).alias("duration_sec_per_bpm_percent_rank"),
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id_by_dur).alias("running_count"),
        F.min("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_bpm"),
        F.min("bpm").over(w_is_explicit_artist_id_by_dur).alias("min_bpm_so_far"),
        F.row_number().over(w_is_explicit_artist_id_by_dur_2).alias("rn"),
        F.round(F.col("duration_sec_per_bpm") - F.avg("duration_sec_per_bpm").over(w_is_explicit_artist_id), 2).alias("duration_sec_per_bpm_vs_is_explicit_artist_id_av"),
        F.round(F.avg("duration_sec_per_bpm").over(w_artist_id_by_duration_sec_mo), 2).alias("duration_sec_per_bpm_avg_7"),
        F.round(F.percent_rank().over(w_is_explicit_by_bpm), 3).alias("bpm_percent_rank"),
        F.nth_value("duration_sec_per_bpm", 2).over(w_is_explicit_artist_id_by_dur_3).alias("second_duration_sec_per_bpm"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_duration_sec)).alias("bpm_change"),
        F.min("duration_sec").over(w_all_by_duration_sec_per_bpm_2).alias("min_duration_sec_so_far"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_by_bpm_ties), 3).alias("bpm_percent_rank_2"),
        F.dense_rank().over(w_all_by_duration_sec_per_bpm).alias("duration_sec_per_bpm_dense_rank"),
    )
)

top_per_group = (
    with_trends
    .filter((F.col("duration_sec_per_bpm_dense_rank") == 1) & (F.col("bpm").between(73, 156)))
    .drop("duration_sec_per_bpm_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_per_bpm_percent_rank",
        "is_explicit_artist_id_max_bpm",
        "is_explicit_artist_id_min_bpm",
        "min_bpm_so_far",
        "rn",
        "duration_sec_per_bpm_vs_is_explicit_artist_id_av",
        "duration_sec_per_bpm_avg_7",
        "bpm_percent_rank",
        "bpm_change",
        "min_duration_sec_so_far",
        "is_explicit_artist_id_avg_bpm",
        "bpm_percent_rank_2",
        (F.floor(F.col("duration_sec_per_bpm") / 2) * 2).alias("duration_sec_per_bpm_bucket"),
    )
    .orderBy(F.col("min_duration_sec_so_far").desc_nulls_first())
)
