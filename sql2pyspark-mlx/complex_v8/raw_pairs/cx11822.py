from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_title_len = Window.partitionBy("title_len")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_prior_3 = Window.orderBy("duration_sec", "id").rowsBetween(-3, -1)
w_all_by_title_len = Window.orderBy(F.col("title_len").desc(), "id")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_duration_sec_vs_title = Window.orderBy(F.col("duration_sec_vs_title_len_avg").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_title_len_dense_rank_by_dura = Window.partitionBy("title_len_dense_rank").orderBy(F.col("duration_sec_share_of_title_len").asc_nulls_last(), "id")
w_title_len_filled = Window.partitionBy("title_len_filled")

base_tracks = (
    df_tracks
    .filter(F.col("bpm").between(76, 147))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

sequenced = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "title_ref",
        "title_len",
        F.round(F.col("title_len") - F.avg("title_len").over(w_title_len), 2).alias("title_len_vs_title_len_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_title_len), F.lit(0)), 4).alias("duration_sec_share_of_title_len"),
        F.size(F.collect_set("artist_id").over(w_title_len)).alias("distinct_artist_id_in_title_len"),
        F.last("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("last_known_release_date"),
        F.round(F.avg("title_len").over(w_all_by_duration_sec_prior_3), 2).alias("title_len_avg_3_prior"),
        F.round(F.avg("duration_sec").over(w_title_len), 2).alias("title_len_avg_duration_sec"),
        F.dense_rank().over(w_all_by_title_len).alias("title_len_dense_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_bpm_running).alias("first_known_duration_sec"),
        F.rank().over(w_all_by_duration_sec_ties).alias("duration_sec_rank"),
        F.round(F.avg("bpm").over(w_title_len), 2).alias("title_len_avg_bpm"),
        F.max("bpm").over(w_title_len).alias("title_len_max_bpm"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_cume_dist"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_title_len), 2).alias("duration_sec_vs_title_len_avg"),
    )
)

annotated = (
    sequenced
    .select(
        "id",
        "release_date",
        "title_ref",
        "title_len_vs_title_len_avg",
        "duration_sec_share_of_title_len",
        "title_len_avg_3_prior",
        "title_len_avg_duration_sec",
        "title_len_dense_rank",
        "first_known_duration_sec",
        "duration_sec_vs_title_len_avg",
        F.round(F.col("title_len_avg_3_prior") / F.nullif(F.col("duration_sec_cume_dist"), F.lit(0)), 3).alias("title_len_avg_3_prior_per_duration_sec_cume_dist"),
        F.coalesce(F.col("title_len"), F.lit(0)).alias("title_len_filled"),
    )
)

with_trends = (
    annotated
    .select(
        "*",
        F.nth_value("first_known_duration_sec", 2).over(w_all_by_duration_sec_vs_title).alias("second_first_known_duration_sec"),
        F.lag("release_date", 2).over(w_title_len_dense_rank_by_dura).alias("prev2_release_date"),
        F.round(F.avg("title_len_filled").over(w_title_len_filled), 2).alias("title_len_filled_avg_title_len_filled"),
    )
)

result = (
    with_trends
    .filter(F.col("title_ref").isNotNull())
    .select(
        "id",
        "release_date",
        "title_ref",
        "duration_sec_share_of_title_len",
        "title_len_avg_3_prior",
        "title_len_avg_duration_sec",
        "title_len_dense_rank",
        "first_known_duration_sec",
        "duration_sec_vs_title_len_avg",
        "title_len_avg_3_prior_per_duration_sec_cume_dist",
        "second_first_known_duration_sec",
        "title_len_filled_avg_title_len_filled",
        F.length(F.trim(F.col("title_ref"))).alias("title_ref_len"),
        F.coalesce(F.col("title_len_filled"), F.lit(0)).alias("title_len_filled_filled"),
    )
    .orderBy("title_ref")
)
