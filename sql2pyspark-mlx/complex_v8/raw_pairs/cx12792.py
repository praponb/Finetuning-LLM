from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_moving_1 = Window.orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id")
w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_rank_release_da = Window.partitionBy("duration_sec_rank", F.month("release_date")).orderBy(F.col("duration_sec_rank").desc(), "id").rowsBetween(-2, -1)
w_all_by_duration_sec_ties = Window.orderBy(F.col("duration_sec").desc_nulls_first())
w_all_by_duration_sec_2 = Window.orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_all_by_duration_sec_rank = Window.orderBy(F.col("duration_sec_rank").desc(), "id")
w_artist_id_duration_sec_dense = Window.partitionBy("artist_id", "duration_sec_dense_rank")
w_duration_sec_rank_release_da_2 = Window.partitionBy("duration_sec_rank", F.month("release_date")).orderBy(F.col("duration_sec_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_rank_release_da_3 = Window.partitionBy("duration_sec_rank", F.month("release_date")).orderBy(F.col("duration_sec_rank").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (~F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

sequenced = (
    recent_tracks
    .select(
        "*",
        F.round(F.avg("duration_sec").over(w_all_by_release_date_moving_1), 2).alias("duration_sec_avg_2"),
        F.sum("duration_sec").over(w_is_explicit).alias("is_explicit_sum_duration_sec"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.rank().over(w_is_explicit_by_duration_sec).alias("duration_sec_rank"),
        F.dense_rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_dense_rank"),
        F.ntile(3).over(w_all_by_duration_sec).alias("duration_sec_tercile"),
        F.last("duration_sec", ignorenulls=True).over(w_artist_id_by_duration_sec_ru).alias("last_known_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id_is_explicit), 2).alias("duration_sec_vs_artist_id_is_explicit_avg"),
    )
)

derived = (
    sequenced
    .filter(F.col("rows_in_is_explicit") > 3)
    .select(
        "*",
        (F.floor(F.col("duration_sec_rank") / 2) * 2).alias("duration_sec_rank_bucket"),
        F.date_add(F.col("release_date"), 30).alias("release_date_plus_30d"),
    )
)

with_windows = (
    derived
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "duration_sec_avg_2",
        "is_explicit_sum_duration_sec",
        "duration_sec_rank",
        "duration_sec_tercile",
        "last_known_duration_sec",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "duration_sec_rank_bucket",
        "release_date_plus_30d",
        F.sum("duration_sec_avg_2").over(w_duration_sec_rank_release_da).alias("duration_sec_avg_2_sum_2_prior"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
        (F.col("duration_sec_tercile") - F.lag("duration_sec_tercile").over(w_all_by_duration_sec_2)).alias("duration_sec_tercile_change"),
        F.lag("duration_sec").over(w_all_by_duration_sec_rank).alias("prev_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id_duration_sec_dense).alias("rows_in_artist_id_duration_sec_dense_rank"),
        F.round(F.col("duration_sec_tercile") - F.avg("duration_sec_tercile").over(w_artist_id_duration_sec_dense), 2).alias("duration_sec_tercile_vs_artist_id_duration_sec_d"),
        F.last("duration_sec_rank_bucket", ignorenulls=True).over(w_duration_sec_rank_release_da_2).alias("last_known_duration_sec_rank_bucket"),
        F.nth_value("release_date_plus_30d", 2).over(w_duration_sec_rank_release_da_3).alias("second_release_date_plus_30d"),
    )
)

result = (
    with_windows
    .filter(F.col("duration_sec_tercile_vs_artist_id_duration_sec_d") > -3.0)
    .select(
        "id",
        "artist_id",
        "release_date",
        "duration_sec_avg_2",
        "is_explicit_sum_duration_sec",
        "duration_sec_rank",
        "duration_sec_tercile",
        "last_known_duration_sec",
        "duration_sec_vs_artist_id_is_explicit_avg",
        "duration_sec_rank_bucket",
        "release_date_plus_30d",
        "duration_sec_avg_2_sum_2_prior",
        "duration_sec_percent_rank",
        "prev_duration_sec",
        "rows_in_artist_id_duration_sec_dense_rank",
        "second_release_date_plus_30d",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
    .orderBy(F.col("prev_duration_sec").asc(), "id")
    .limit(10)
)
