from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country = Window.partitionBy("artist_country")
w_artist_country_by_duration_s = Window.partitionBy("artist_country").orderBy(F.col("duration_sec").desc(), "id")
w_artist_country_by_duration_s_2 = Window.partitionBy("artist_country").orderBy("duration_sec", "id")
w_artist_country_artist_id_by = Window.partitionBy("artist_country", "artist_id").orderBy("bpm", "id")
w_artist_country_by_duration_s_3 = Window.partitionBy("artist_country").orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_all_by_duration_sec_percent = Window.orderBy("duration_sec_percent_rank", "id")
w_artist_id_by_bpm_vs_artist_c = Window.partitionBy("artist_id").orderBy(F.col("bpm_vs_artist_country_avg").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_vs_artist_c_2 = Window.partitionBy("artist_id").orderBy(F.col("bpm_vs_artist_country_avg").asc_nulls_last())

prepared = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "bpm",
        "moods",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_details = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm",
        "s.moods",
        "s.days_since_release_date",
        F.col("a.country").alias("artist_country"),
    )
)

with_windows = (
    with_details
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_country), 2).alias("bpm_vs_artist_country_avg"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_country), F.lit(0)), 4).alias("bpm_share_of_artist_country"),
        F.lead("bpm", 1, 0).over(w_artist_country_by_duration_s).alias("next_bpm"),
        F.row_number().over(w_artist_country_by_duration_s).alias("seq_no"),
        F.round(F.percent_rank().over(w_artist_country_by_duration_s_2), 3).alias("duration_sec_percent_rank"),
        F.lag("days_since_release_date", 3).over(w_artist_country_artist_id_by).alias("prev3_days_since_release_date"),
        F.round(F.col("days_since_release_date") / F.nullif(F.sum("days_since_release_date").over(w_artist_country), F.lit(0)), 4).alias("days_since_release_date_share_of_artist_country"),
        F.min("days_since_release_date").over(w_artist_country_by_duration_s_3).alias("min_days_since_release_date_so_far"),
    )
)

ranked = (
    with_windows
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "days_since_release_date",
        "artist_country",
        "bpm_vs_artist_country_avg",
        "bpm_share_of_artist_country",
        "next_bpm",
        "duration_sec_percent_rank",
        "days_since_release_date_share_of_artist_country",
        "min_days_since_release_date_so_far",
        F.rank().over(w_all_by_duration_sec).alias("duration_sec_rank"),
        F.round(F.cume_dist().over(w_all_by_duration_sec_percent), 3).alias("duration_sec_percent_rank_cume_dist"),
        F.last("bpm", ignorenulls=True).over(w_artist_id_by_bpm_vs_artist_c).alias("last_known_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.sum("days_since_release_date").over(w_artist_id_by_bpm_vs_artist_c).alias("running_days_since_release_date"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_duration_sec)).alias("bpm_change"),
        F.size(F.collect_set("artist_country").over(w_artist_id)).alias("distinct_artist_country_in_artist_id"),
        F.dense_rank().over(w_artist_id_by_bpm_vs_artist_c_2).alias("bpm_vs_artist_country_avg_dense_rank"),
    )
)

result = (
    ranked
    .filter(F.col("distinct_artist_country_in_artist_id") <= 7)
    .select(
        "id",
        "artist_id",
        "bpm",
        "moods",
        "days_since_release_date",
        "artist_country",
        "bpm_vs_artist_country_avg",
        "bpm_share_of_artist_country",
        "days_since_release_date_share_of_artist_country",
        "min_days_since_release_date_so_far",
        "duration_sec_percent_rank_cume_dist",
        "last_known_bpm",
        "bpm_share_of_artist_id",
        "running_days_since_release_date",
        "bpm_change",
        "distinct_artist_country_in_artist_id",
        "bpm_vs_artist_country_avg_dense_rank",
        F.coalesce(F.col("artist_country"), F.lit("UNKNOWN")).alias("artist_country_or_unknown"),
    )
    .orderBy(
        F.col("bpm_change").desc(),
        F.col("bpm_share_of_artist_country").desc_nulls_first(),
    )
)
