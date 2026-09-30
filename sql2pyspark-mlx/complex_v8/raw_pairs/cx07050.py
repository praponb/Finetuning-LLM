from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_bpm = Window.orderBy(F.col("bpm").desc(), "id")
w_title_len_artist_id = Window.partitionBy("title_len", "artist_id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_title_len_by_title_len_runni = Window.partitionBy("title_len").orderBy(F.col("title_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_bpm_moving_1 = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, Window.currentRow)
w_title_len_by_title_len = Window.partitionBy("title_len").orderBy(F.col("title_len").desc(), "id")
w_all_by_bpm_2 = Window.orderBy("bpm", "id")
w_title_len_filled_by_rows_in = Window.partitionBy("title_len_filled").orderBy(F.col("rows_in_title_len_artist_id").desc(), "id")
w_last_title_len_is_explicit = Window.partitionBy("last_title_len", "is_explicit")
w_title_len_filled_by_rows_in_2 = Window.partitionBy("title_len_filled").orderBy(F.col("rows_in_title_len_artist_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_full_2 = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tracks = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("P%"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
    )
)

windowed = (
    filtered_tracks
    .select(
        "*",
        F.ntile(4).over(w_all_by_bpm).alias("bpm_quartile"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_title_len_artist_id), F.lit(0)), 4).alias("bpm_share_of_title_len_artist_id"),
        F.nth_value("least_duration_sec_bpm", 2).over(w_all_by_bpm_full).alias("second_least_duration_sec_bpm"),
        F.row_number().over(w_artist_id_by_bpm).alias("rn"),
        F.first("bpm", ignorenulls=True).over(w_title_len_by_title_len_runni).alias("first_known_bpm"),
        F.round(F.percent_rank().over(w_all_by_bpm), 3).alias("bpm_percent_rank"),
        F.last("title_len").over(w_all_by_bpm_full).alias("last_title_len"),
        F.round(F.avg("title_len").over(w_all_by_bpm_moving_1), 2).alias("title_len_avg_2"),
        F.count(F.lit(1)).over(w_title_len_artist_id).alias("rows_in_title_len_artist_id"),
        (F.col("least_duration_sec_bpm") - F.lag("least_duration_sec_bpm").over(w_title_len_by_title_len)).alias("least_duration_sec_bpm_change"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.when(F.col("title_len") >= 11, "large").when(F.col("title_len") >= 7, "medium").otherwise("small").alias("title_len_tier"),
        F.coalesce(F.col("title_len"), F.lit(0)).alias("title_len_filled"),
    )
)

ranked = (
    scored
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_bpm_2), 3).alias("bpm_percent_rank_2"),
        F.rank().over(w_title_len_filled_by_rows_in).alias("rows_in_title_len_artist_id_rank"),
        F.sum("bpm_percent_rank").over(w_last_title_len_is_explicit).alias("last_title_len_is_explicit_sum_bpm_percent_rank"),
        F.count(F.lit(1)).over(w_last_title_len_is_explicit).alias("rows_in_last_title_len_is_explicit"),
        F.dense_rank().over(w_title_len_filled_by_rows_in).alias("rows_in_title_len_artist_id_dense_rank"),
        F.first("rows_in_title_len_artist_id").over(w_title_len_filled_by_rows_in_2).alias("first_rows_in_title_len_artist_id"),
        F.first("title_len_filled").over(w_all_by_bpm_full_2).alias("first_title_len_filled"),
    )
)

summary = (
    ranked
    .filter(F.col("title_len_tier") == "large")
    .groupBy("bpm_quartile")
    .agg(
        F.max("last_title_len").alias("max_last_title_len"),
        F.count("*").alias("cnt"),
    )
)

result = (
    summary
    .select(
        "bpm_quartile",
        "max_last_title_len",
        F.greatest(F.col("cnt"), F.col("max_last_title_len")).alias("greatest_cnt_max_last_title_len"),
        F.abs(F.col("cnt") - 8).alias("cnt_distance"),
    )
    .orderBy(F.col("max_last_title_len").asc())
)
