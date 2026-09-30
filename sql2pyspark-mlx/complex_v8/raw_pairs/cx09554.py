from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_is_explicit_flag_full = Window.orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_is_explicit_flag = Window.orderBy("is_explicit_flag", "id")
w_all_by_is_explicit_flag_runn = Window.orderBy("is_explicit_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_id_is_explicit = Window.orderBy(F.col("artist_id_is_explicit_sum_greatest_bpm_duration_").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_artist_id_is_explicit_2 = Window.orderBy(F.col("artist_id_is_explicit_sum_greatest_bpm_duration_").desc(), "id").rowsBetween(-1, -1)
w_all_by_artist_id_is_explicit_3 = Window.orderBy(F.col("artist_id_is_explicit_sum_greatest_bpm_duration_").desc(), "id")
w_all_by_artist_id_is_explicit_4 = Window.orderBy("artist_id_is_explicit_sum_greatest_bpm_duration__2", "id")
w_all_by_artist_id_is_explicit_5 = Window.orderBy("artist_id_is_explicit_sum_greatest_bpm_duration__2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
)

with_windows = (
    recent_tracks
    .select(
        "*",
        F.first("release_date").over(w_all_by_is_explicit_flag_full).alias("first_release_date"),
        F.sum("greatest_bpm_duration_sec").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_sum_greatest_bpm_duration_"),
        (F.col("is_explicit_flag") - F.lag("is_explicit_flag").over(w_all_by_is_explicit_flag)).alias("is_explicit_flag_change"),
        F.count(F.lit(1)).over(w_all_by_is_explicit_flag_runn).alias("running_count"),
        F.row_number().over(w_all_by_is_explicit_flag).alias("seq_no"),
    )
)

screened = (
    with_windows.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date").between(F.lit("2024-04-29").cast("date"), F.lit("2025-04-02").cast("date"))) & (F.col("t.duration_sec") > 165), "left_anti")
    .filter(F.col("is_explicit_flag_change") < 0)
)

ranked = (
    screened
    .select(
        "*",
        F.min("is_explicit_flag").over(w_all_by_artist_id_is_explicit).alias("min_is_explicit_flag_so_far"),
        F.sum("greatest_bpm_duration_sec").over(w_all_by_artist_id_is_explicit_2).alias("greatest_bpm_duration_sec_sum_1_prior"),
        F.ntile(10).over(w_all_by_artist_id_is_explicit_3).alias("artist_id_is_explicit_sum_greatest_bpm_duration__2"),
    )
)

labeled = (
    ranked
    .filter(F.col("running_count") < 12)
    .select(
        "*",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.round(F.col("running_count") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("running_count_per_duration_sec"),
        F.month(F.col("release_date")).alias("release_date_month"),
    )
)

metrics = (
    labeled
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "title_clean",
        "greatest_bpm_duration_sec",
        "is_explicit_flag",
        "first_release_date",
        "running_count",
        "seq_no",
        "min_is_explicit_flag_so_far",
        "greatest_bpm_duration_sec_sum_1_prior",
        "running_count_per_duration_sec",
        "release_date_month",
        F.rank().over(w_all_by_artist_id_is_explicit_4).alias("artist_id_is_explicit_sum_greatest_bpm_duration__3"),
        F.max("title_len").over(w_all_by_artist_id_is_explicit_5).alias("max_title_len_so_far"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "greatest_bpm_duration_sec",
        "is_explicit_flag",
        "first_release_date",
        "seq_no",
        "greatest_bpm_duration_sec_sum_1_prior",
        "running_count_per_duration_sec",
        "release_date_month",
        "artist_id_is_explicit_sum_greatest_bpm_duration__3",
    )
)
