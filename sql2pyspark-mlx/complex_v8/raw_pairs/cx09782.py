from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_quarter_by_bpm = Window.partitionBy("release_date_quarter").orderBy("bpm", "id").rowsBetween(-3, -1)
w_release_date_quarter_artist = Window.partitionBy("release_date_quarter", "artist_id").orderBy(F.col("release_date_quarter").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_all_by_release_date_quarter = Window.orderBy(F.col("release_date_quarter").asc_nulls_last(), "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_release_date_quarter_2 = Window.orderBy(F.col("release_date_quarter").asc_nulls_last())
w_release_date_quarter_by_bpm_2 = Window.partitionBy("release_date_quarter").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_quarter_by_bpm_3 = Window.partitionBy("release_date_quarter").orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_second_release_date_quarter = Window.partitionBy("second_release_date_quarter_2").orderBy(F.col("prev_bpm").desc_nulls_first())
w_is_explicit = Window.partitionBy("is_explicit")
w_release_date_quarter_is_expl = Window.partitionBy("release_date_quarter", "is_explicit").orderBy(F.col("prev_bpm").desc())

recent = (
    df_tracks
    .filter((F.col("bpm") <= 165) | (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.sum("duration_sec").over(w_release_date_quarter_by_bpm).alias("duration_sec_sum_3_prior"),
        F.nth_value("duration_sec", 2).over(w_release_date_quarter_artist).alias("second_duration_sec"),
        F.sum("release_date_quarter").over(w_artist_id).alias("artist_id_sum_release_date_quarter"),
        F.dense_rank().over(w_all_by_release_date_quarter).alias("release_date_quarter_dense_rank"),
        F.nth_value("duration_sec", 2).over(w_all_by_bpm_full).alias("second_duration_sec_2"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.round(F.percent_rank().over(w_all_by_release_date_quarter_2), 3).alias("release_date_quarter_percent_rank"),
        F.nth_value("release_date_quarter", 2).over(w_release_date_quarter_by_bpm_2).alias("second_release_date_quarter"),
        F.lag("bpm").over(w_all_by_release_date_quarter).alias("prev_bpm"),
        F.round(F.avg("bpm").over(w_release_date_quarter_by_bpm_3), 2).alias("bpm_avg_7"),
        F.first("release_date_quarter").over(w_release_date_quarter_artist).alias("first_release_date_quarter"),
        F.nth_value("release_date_quarter", 2).over(w_release_date_quarter_artist).alias("second_release_date_quarter_2"),
    )
)

leaders = (
    metrics
    .filter(F.col("release_date_quarter_dense_rank") == 1)
    .drop("release_date_quarter_dense_rank")
)

with_trends = (
    leaders
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "release_date_quarter",
        "second_duration_sec",
        "artist_id_sum_release_date_quarter",
        "rows_in_artist_id",
        "second_release_date_quarter",
        "bpm_avg_7",
        "second_release_date_quarter_2",
        F.round(F.percent_rank().over(w_second_release_date_quarter), 3).alias("prev_bpm_percent_rank"),
        F.size(F.collect_set("artist_id").over(w_is_explicit)).alias("distinct_artist_id_in_is_explicit"),
        F.round(F.col("second_duration_sec") - F.avg("second_duration_sec").over(w_is_explicit), 2).alias("second_duration_sec_vs_is_explicit_avg"),
        F.dense_rank().over(w_release_date_quarter_is_expl).alias("prev_bpm_dense_rank"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
        "second_duration_sec",
        "artist_id_sum_release_date_quarter",
        "rows_in_artist_id",
        "second_release_date_quarter",
        "bpm_avg_7",
        "second_release_date_quarter_2",
        "prev_bpm_percent_rank",
        "distinct_artist_id_in_is_explicit",
        "prev_bpm_dense_rank",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
    )
    .orderBy(F.col("distinct_artist_id_in_is_explicit").desc())
)
