from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_month = Window.partitionBy("artist_id", F.date_trunc("month", "release_date"))
w_release_date_month_by_durati = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_durati_2 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec")
w_release_date_month_by_durati_3 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy("duration_sec", "id")
w_artist_id_release_date_month_2 = Window.partitionBy("artist_id", F.month("release_date")).orderBy(F.col("release_date").desc_nulls_first(), "id")
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date_ti = Window.partitionBy("artist_id").orderBy("release_date")
w_artist_id_by_release_date_ru = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_durati_4 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy("duration_sec", "id").rowsBetween(-2, -1)

valid_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
)

windowed = (
    valid_tracks
    .select(
        "*",
        F.sum("duration_sec").over(w_release_date_month_by_durati).alias("running_duration_sec"),
        F.dense_rank().over(w_release_date_month_by_durati_2).alias("duration_sec_dense_rank"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_release_date_month), 2).alias("duration_sec_vs_artist_id_release_date_month_avg"),
        F.round(F.avg("duration_sec").over(w_release_date_month_by_durati_3), 2).alias("duration_sec_avg_6_prior"),
        F.ntile(3).over(w_artist_id_release_date_year).alias("duration_sec_tercile"),
        F.row_number().over(w_artist_id_release_date_month_2).alias("release_date_seq"),
        F.sum("duration_sec").over(w_artist_id_release_date_month).alias("artist_id_release_date_month_sum_duration_sec"),
        F.last("duration_sec").over(w_artist_id_by_release_date_fu).alias("last_duration_sec"),
        F.rank().over(w_artist_id_by_release_date_ti).alias("release_date_rank"),
        F.max("duration_sec").over(w_artist_id_by_release_date_ru).alias("max_duration_sec_so_far"),
        F.lag("release_date").over(w_artist_id_release_date_month_2).alias("prev_release_date"),
        F.round(F.avg("duration_sec").over(w_release_date_month_by_durati_4), 2).alias("duration_sec_avg_2_prior"),
    )
)

scored = (
    windowed
    .select(
        "*",
        F.substring(F.trim(F.col("title_prefix")), 1, 4).alias("title_prefix_prefix"),
    )
)

result = (
    scored
    .filter(F.col("last_duration_sec") < 312)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "moods",
        "title_prefix",
        "duration_sec_vs_artist_id_release_date_month_avg",
        "duration_sec_avg_6_prior",
        "duration_sec_tercile",
        "release_date_seq",
        "last_duration_sec",
        "release_date_rank",
        "max_duration_sec_so_far",
        "prev_release_date",
        "duration_sec_avg_2_prior",
        "title_prefix_prefix",
        F.concat(F.col("title_prefix"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_prefix_ref"),
    )
    .orderBy(F.col("release_date_seq").desc())
)
