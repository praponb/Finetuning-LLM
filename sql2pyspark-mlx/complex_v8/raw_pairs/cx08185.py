from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_all_by_greatest_duration_sec = Window.orderBy("greatest_duration_sec_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_id = Window.partitionBy(F.year("release_date"), "artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_id_2 = Window.partitionBy(F.year("release_date"), "artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id")
w_all_by_greatest_duration_sec_2 = Window.orderBy("greatest_duration_sec_bpm")
w_all_by_greatest_duration_sec_3 = Window.orderBy("greatest_duration_sec_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date")).orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_te = Window.partitionBy("artist_id").orderBy(F.col("release_date_tercile").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_greatest_duration_sec_bpm_ra = Window.partitionBy("greatest_duration_sec_bpm_rank").orderBy(F.col("greatest_duration_sec_bpm_rank").desc(), "id")
w_all_by_rows_in_release_date = Window.orderBy("rows_in_release_date_month", "id")
w_greatest_duration_sec_bpm_ra_2 = Window.partitionBy("greatest_duration_sec_bpm_rank", "release_date_tercile")
w_artist_id_by_release_date_te_2 = Window.partitionBy("artist_id").orderBy(F.col("release_date_tercile").desc_nulls_first(), "id")

prepared_tracks = (
    df_tracks
    .filter((F.col("bpm") >= 100) & (F.col("duration_sec") > 165))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        F.greatest(F.col("duration_sec"), F.col("bpm")).alias("greatest_duration_sec_bpm"),
    )
)

with_windows = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "moods",
        F.count(F.lit(1)).over(w_release_date_month).alias("rows_in_release_date_month"),
        F.last("greatest_duration_sec_bpm", ignorenulls=True).over(w_all_by_greatest_duration_sec).alias("last_known_greatest_duration_sec_bpm"),
        F.count(F.lit(1)).over(w_all_by_greatest_duration_sec).alias("running_count"),
        F.last("greatest_duration_sec_bpm", ignorenulls=True).over(w_release_date_year_artist_id).alias("last_known_greatest_duration_sec_bpm_2"),
        F.ntile(3).over(w_release_date_year_artist_id_2).alias("release_date_tercile"),
        F.rank().over(w_all_by_greatest_duration_sec_2).alias("greatest_duration_sec_bpm_rank"),
        F.last("greatest_duration_sec_bpm").over(w_all_by_greatest_duration_sec_3).alias("last_greatest_duration_sec_bpm"),
        F.min("greatest_duration_sec_bpm").over(w_release_date_month).alias("release_date_month_min_greatest_duration_sec_bpm"),
        F.first("release_date", ignorenulls=True).over(w_artist_id_release_date_month).alias("first_known_release_date"),
    )
)

sequenced = (
    with_windows
    .select(
        "*",
        F.sum("running_count").over(w_artist_id_by_release_date_te).alias("running_count_sum_2_prior"),
        F.rank().over(w_greatest_duration_sec_bpm_ra).alias("greatest_duration_sec_bpm_rank_rank"),
        F.lead("running_count").over(w_greatest_duration_sec_bpm_ra).alias("next_running_count"),
        F.lead("rows_in_release_date_month").over(w_all_by_rows_in_release_date).alias("next_rows_in_release_date_month"),
        F.count(F.lit(1)).over(w_greatest_duration_sec_bpm_ra_2).alias("rows_in_greatest_duration_sec_bpm_rank_release_d"),
        F.min("rows_in_release_date_month").over(w_greatest_duration_sec_bpm_ra_2).alias("greatest_duration_sec_bpm_rank_release_date_terc"),
        (F.col("running_count") - F.lag("running_count").over(w_artist_id_by_release_date_te_2)).alias("running_count_change"),
    )
)

result = (
    sequenced
    .select(
        "running_count_sum_2_prior",
        "next_rows_in_release_date_month",
        "first_known_release_date",
    )
    .distinct()
    .orderBy(
        F.col("first_known_release_date").desc(),
        F.col("running_count_sum_2_prior").desc(),
    )
)
