from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_bpm = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_2 = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_bpm_full_2 = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_ties = Window.orderBy("bpm")

recent = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

sequenced = (
    recent
    .select(
        "id",
        "duration_sec",
        "bpm",
        "title_clean",
        (F.col("bpm") - F.lag("bpm").over(w_release_date_month_by_bpm)).alias("bpm_change"),
        F.round(F.avg("bpm").over(w_release_date_month), 2).alias("release_date_month_avg_bpm"),
        F.lead("bpm", 2).over(w_artist_id_by_bpm).alias("next2_bpm"),
        F.lag("bpm").over(w_artist_id_by_bpm).alias("prev_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_year_artist_id), F.lit(0)), 4).alias("bpm_share_of_release_date_year_artist_id"),
        F.last("duration_sec").over(w_artist_id_by_bpm_full).alias("last_duration_sec"),
        F.row_number().over(w_artist_id_by_bpm_2).alias("bpm_seq"),
        F.nth_value("bpm", 2).over(w_artist_id_by_bpm_full_2).alias("second_bpm"),
        F.round(F.percent_rank().over(w_artist_id_by_bpm), 3).alias("bpm_percent_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_release_date_year_artist_id), 2).alias("duration_sec_vs_release_date_year_artist_id_avg"),
        F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_release_date_month), F.lit(0)), 4).alias("duration_sec_share_of_release_date_month"),
        F.round(F.percent_rank().over(w_all_by_bpm_ties), 3).alias("bpm_percent_rank_2"),
        F.count(F.lit(1)).over(w_release_date_year_artist_id).alias("rows_in_release_date_year_artist_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "duration_sec",
        "bpm",
        "bpm_change",
        "release_date_month_avg_bpm",
        "next2_bpm",
        "prev_bpm",
        "bpm_share_of_release_date_year_artist_id",
        "second_bpm",
        "duration_sec_share_of_release_date_month",
        "rows_in_release_date_year_artist_id",
    )
    .orderBy("bpm_change")
)
