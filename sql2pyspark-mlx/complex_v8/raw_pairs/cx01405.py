from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy(F.col("duration_sec").desc(), "id")
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy(F.col("duration_sec").desc())
w_release_date_month_by_bpm_ru = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_pr = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)
w_release_date_month_by_bpm_mo = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(-1, Window.currentRow)
w_release_date_month_by_bpm = Window.partitionBy(F.month("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id")

base_tracks = (
    df_tracks
    .filter(F.col("artist_id") >= 3)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

sequenced = (
    base_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        "moods",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_release_date_month), F.lit(0)), 4).alias("bpm_share_of_release_date_month"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id_is_explicit), 2).alias("bpm_vs_artist_id_is_explicit_avg"),
        F.sum("bpm").over(w_artist_id_by_release_date_ru).alias("running_bpm"),
        F.lag("release_date").over(w_release_date_year_artist_id).alias("prev_release_date"),
        F.dense_rank().over(w_release_date_year_artist_id_2).alias("duration_sec_dense_rank"),
        F.max("duration_sec").over(w_release_date_month).alias("release_date_month_max_duration_sec"),
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.sum("duration_sec").over(w_release_date_month).alias("release_date_month_sum_duration_sec"),
        F.first("bpm", ignorenulls=True).over(w_release_date_month_by_bpm_ru).alias("first_known_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_by_release_date_pr), 2).alias("bpm_avg_1_prior"),
        F.round(F.cume_dist().over(w_release_date_year_artist_id_2), 3).alias("duration_sec_cume_dist"),
        F.round(F.avg("bpm").over(w_release_date_month_by_bpm_mo), 2).alias("bpm_avg_2"),
        F.lead("duration_sec").over(w_release_date_month_by_bpm).alias("next_duration_sec"),
    )
)

top_per_group = (
    sequenced
    .filter(F.col("duration_sec_dense_rank") <= 5)
    .drop("duration_sec_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "bpm_share_of_release_date_month",
        "rows_in_artist_id_is_explicit",
        "bpm_vs_artist_id_is_explicit_avg",
        "prev_release_date",
        "release_date_month_max_duration_sec",
        "release_date_month_sum_duration_sec",
        "first_known_bpm",
        "bpm_avg_1_prior",
        "duration_sec_cume_dist",
        "bpm_avg_2",
        "next_duration_sec",
        F.coalesce(F.col("next_duration_sec"), F.lit(0)).alias("next_duration_sec_filled"),
    )
    .orderBy(F.col("next_duration_sec").desc(), F.col("artist_id").desc())
)
