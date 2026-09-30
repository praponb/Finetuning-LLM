from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_flag = Window.partitionBy("is_explicit_flag")
w_artist_id_by_is_explicit_fla = Window.partitionBy("artist_id").orderBy(F.col("is_explicit_flag").desc(), "id")
w_is_explicit_flag_artist_id_b = Window.partitionBy("is_explicit_flag", "artist_id").orderBy(F.col("is_explicit_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_flag_by_duration = Window.partitionBy("is_explicit_flag").orderBy("duration_sec")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_flag_artist_id_b_2 = Window.partitionBy("is_explicit_flag", "artist_id").orderBy(F.col("is_explicit_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_by_bpm_full = Window.partitionBy("is_explicit_flag").orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_is_explicit_fla_2 = Window.partitionBy("artist_id").orderBy(F.col("is_explicit_flag").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_flag_artist_id_b_3 = Window.partitionBy("is_explicit_flag", "artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_first_is_explicit_flag_disti = Window.partitionBy("first_is_explicit_flag", "distinct_artist_id_in_is_explicit_flag")
w_artist_id_is_explicit_flag_b = Window.partitionBy("artist_id", "is_explicit_flag").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_artist_id_is_explicit_flag_b_2 = Window.partitionBy("artist_id", "is_explicit_flag").orderBy("duration_sec", "id")
w_is_explicit_flag_rank_is_exp = Window.partitionBy("is_explicit_flag_rank", "is_explicit_flag").orderBy(F.col("bpm").desc(), "id").rowsBetween(-2, -1)

valid_tracks = (
    df_tracks
    .filter(F.col("bpm").between(73, 155))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

metrics = (
    valid_tracks
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_is_explicit_flag)).alias("distinct_artist_id_in_is_explicit_flag"),
        F.rank().over(w_artist_id_by_is_explicit_fla).alias("is_explicit_flag_rank"),
        F.last("bpm", ignorenulls=True).over(w_is_explicit_flag_artist_id_b).alias("last_known_bpm"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_is_explicit_flag), 2).alias("duration_sec_vs_is_explicit_flag_avg"),
        F.count(F.lit(1)).over(w_is_explicit_flag).alias("rows_in_is_explicit_flag"),
        F.round(F.cume_dist().over(w_is_explicit_flag_by_duration), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4).alias("duration_sec_share_of_artist_id"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.last("bpm").over(w_is_explicit_flag_artist_id_b_2).alias("last_bpm"),
        F.first("is_explicit_flag").over(w_is_explicit_flag_by_bpm_full).alias("first_is_explicit_flag"),
        F.first("bpm").over(w_artist_id_by_is_explicit_fla_2).alias("first_bpm"),
        F.sum("duration_sec").over(w_is_explicit_flag).alias("is_explicit_flag_sum_duration_sec"),
        F.last("bpm").over(w_is_explicit_flag_artist_id_b_3).alias("last_bpm_2"),
    )
)

windowed = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        "distinct_artist_id_in_is_explicit_flag",
        "is_explicit_flag_rank",
        "last_known_bpm",
        "duration_sec_vs_is_explicit_flag_avg",
        "rows_in_is_explicit_flag",
        "duration_sec_cume_dist",
        "duration_sec_share_of_artist_id",
        "artist_id_avg_bpm",
        "first_is_explicit_flag",
        "is_explicit_flag_sum_duration_sec",
        F.min("last_bpm_2").over(w_first_is_explicit_flag_disti).alias("first_is_explicit_flag_distinct_artist_id_in_is_"),
        F.sum("last_known_bpm").over(w_artist_id_is_explicit_flag_b).alias("last_known_bpm_sum_2_prior"),
        F.rank().over(w_artist_id_is_explicit_flag_b_2).alias("duration_sec_rank"),
        F.max("artist_id_avg_bpm").over(w_is_explicit_flag_rank_is_exp).alias("artist_id_avg_bpm_max_2_prior"),
    )
)

result = (
    windowed
    .select(
        "id",
        "duration_sec",
        "distinct_artist_id_in_is_explicit_flag",
        "last_known_bpm",
        "duration_sec_vs_is_explicit_flag_avg",
        "rows_in_is_explicit_flag",
        "duration_sec_cume_dist",
        "duration_sec_share_of_artist_id",
        "last_known_bpm_sum_2_prior",
        "duration_sec_rank",
        F.when(F.col("duration_sec_rank") >= 6, "large").when(F.col("duration_sec_rank") >= 4, "medium").otherwise("small").alias("duration_sec_rank_tier"),
    )
    .orderBy(
        F.col("rows_in_is_explicit_flag").asc_nulls_last(),
        F.col("last_known_bpm").asc_nulls_last(),
    )
)
