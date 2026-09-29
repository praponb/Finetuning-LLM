from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_artist_id_by_duration_sec_pr = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_release_date_pl = Window.partitionBy("artist_id").orderBy(F.col("release_date_plus_1m").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_pl_2 = Window.partitionBy("artist_id").orderBy(F.col("release_date_plus_1m").desc(), "id")
w_release_date_plus_1m_year_ar = Window.partitionBy(F.year("release_date_plus_1m"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, -1)
w_release_date_plus_1m_year_ar_2 = Window.partitionBy(F.year("release_date_plus_1m"), "artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_max_duration_se = Window.partitionBy("artist_id").orderBy("max_duration_sec_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_share_of_ar = Window.partitionBy("artist_id").orderBy("bpm_share_of_artist_id", "id")
w_artist_id_by_max_duration_se_2 = Window.partitionBy("artist_id").orderBy("max_duration_sec_so_far", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_plus_1m_year = Window.partitionBy(F.year("release_date_plus_1m"))
w_artist_id_by_max_duration_se_3 = Window.partitionBy("artist_id").orderBy("max_duration_sec_so_far")

recent_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.add_months(F.col("release_date"), 1).alias("release_date_plus_1m"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

with_trends = (
    recent_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        "release_date_plus_1m",
        "title_prefix",
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        F.max("bpm").over(w_artist_id_by_duration_sec_pr).alias("bpm_max_6_prior"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.max("duration_sec").over(w_artist_id_by_release_date_pl).alias("max_duration_sec_so_far"),
        F.lead("bpm").over(w_artist_id_by_release_date_pl_2).alias("next_bpm"),
        F.round(F.avg("bpm").over(w_release_date_plus_1m_year_ar), 2).alias("bpm_avg_1_prior"),
        F.count(F.lit(1)).over(w_release_date_plus_1m_year_ar_2).alias("running_count"),
    )
)

derived = (
    with_trends
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("title_prefix")), " ", "_")).alias("title_prefix_slug"),
    )
)

windowed = (
    derived
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "release_date_plus_1m",
        "duration_sec_percent_rank",
        "bpm_max_6_prior",
        "bpm_vs_artist_id_avg",
        "bpm_share_of_artist_id",
        "max_duration_sec_so_far",
        "next_bpm",
        "bpm_avg_1_prior",
        "title_prefix_slug",
        F.last("bpm_vs_artist_id_avg").over(w_artist_id_by_max_duration_se).alias("last_bpm_vs_artist_id_avg"),
        F.lag("running_count").over(w_artist_id_by_bpm_share_of_ar).alias("prev_running_count"),
        F.sum("bpm_avg_1_prior").over(w_artist_id_by_max_duration_se_2).alias("running_bpm_avg_1_prior"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_plus_1m_year), F.lit(0)), 4).alias("bpm_share_of_release_date_plus_1m_year"),
        F.dense_rank().over(w_artist_id_by_max_duration_se_3).alias("max_duration_sec_so_far_dense_rank"),
        F.round(F.col("max_duration_sec_so_far") - F.avg("max_duration_sec_so_far").over(w_release_date_plus_1m_year), 2).alias("max_duration_sec_so_far_vs_release_date_plus_1m_"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "release_date_plus_1m",
        "bpm_max_6_prior",
        "bpm_share_of_artist_id",
        "max_duration_sec_so_far",
        "next_bpm",
        "bpm_avg_1_prior",
        "title_prefix_slug",
        "last_bpm_vs_artist_id_avg",
        "prev_running_count",
        "running_bpm_avg_1_prior",
        "max_duration_sec_so_far_dense_rank",
        F.abs(F.col("max_duration_sec_so_far") - 2145).alias("max_duration_sec_so_far_distance"),
        F.concat(F.col("title_prefix_slug"), F.lit("#"), F.col("prev_running_count").cast("string")).alias("title_prefix_slug_ref"),
    )
    .orderBy(
        F.col("max_duration_sec_so_far_dense_rank").asc(),
        F.col("bpm_max_6_prior").asc(),
    )
)
