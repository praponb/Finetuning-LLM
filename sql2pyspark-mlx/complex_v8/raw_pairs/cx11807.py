from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_artist_id_release_date_dayof = Window.partitionBy("artist_id", "release_date_dayofweek").orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_prior_1 = Window.orderBy("bpm", "id").rowsBetween(-1, -1)
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date_dayofweek")
w_release_date_dayofweek_by_re = Window.partitionBy("release_date_dayofweek").orderBy("release_date_quarter", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_release_date_dayof_2 = Window.partitionBy("artist_id", "release_date_dayofweek").orderBy(F.col("bpm").asc_nulls_last(), "id")
w_release_date_dayofweek_by_bp = Window.partitionBy("release_date_dayofweek").orderBy("bpm", "id")
w_release_date_dayofweek_by_bp_2 = Window.partitionBy("release_date_dayofweek").orderBy(F.col("bpm").desc(), "id")
w_release_date_dayofweek_by_bp_3 = Window.partitionBy("release_date_dayofweek").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_running = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_tracks
    .filter((F.col("bpm").between(76, 180)) & (F.lower(F.col("title")).like("% l%")))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

metrics = (
    recent
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        "release_date_dayofweek",
        "release_date_quarter",
        F.max("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_max_bpm"),
        F.rank().over(w_all_by_bpm).alias("bpm_rank"),
        F.first("release_date_dayofweek").over(w_artist_id_release_date_dayof).alias("first_release_date_dayofweek"),
        F.sum("bpm").over(w_all_by_bpm_prior_1).alias("bpm_sum_1_prior"),
        F.round(F.percent_rank().over(w_artist_id_is_explicit_by_rel), 3).alias("release_date_dayofweek_percent_rank"),
        F.nth_value("bpm", 2).over(w_release_date_dayofweek_by_re).alias("second_bpm"),
        F.last("release_date_quarter").over(w_release_date_dayofweek_by_re).alias("last_release_date_quarter"),
        F.first("release_date_quarter").over(w_release_date_dayofweek_by_re).alias("first_release_date_quarter"),
        F.round(F.col("release_date_dayofweek") - F.avg("release_date_dayofweek").over(w_is_explicit_artist_id), 2).alias("release_date_dayofweek_vs_is_explicit_artist_id_"),
        F.row_number().over(w_artist_id_release_date_dayof_2).alias("rn"),
        F.dense_rank().over(w_release_date_dayofweek_by_bp).alias("bpm_dense_rank"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_bpm"),
        F.dense_rank().over(w_release_date_dayofweek_by_bp_2).alias("bpm_dense_rank_2"),
        F.last("release_date_dayofweek").over(w_release_date_dayofweek_by_bp_3).alias("last_release_date_dayofweek"),
        F.first("release_date_dayofweek", ignorenulls=True).over(w_all_by_bpm_running).alias("first_known_release_date_dayofweek"),
    )
)

result = (
    metrics
    .filter(F.col("bpm") > 89)
    .select(
        "id",
        "is_explicit",
        "bpm",
        "release_date_quarter",
        "bpm_rank",
        "first_release_date_dayofweek",
        "bpm_sum_1_prior",
        "release_date_dayofweek_percent_rank",
        "first_release_date_quarter",
        "release_date_dayofweek_vs_is_explicit_artist_id_",
        "rn",
        "bpm_dense_rank",
        "first_known_release_date_dayofweek",
    )
    .orderBy(F.col("bpm").asc_nulls_last(), "bpm_rank")
)
