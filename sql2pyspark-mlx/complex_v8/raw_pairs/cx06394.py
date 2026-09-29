from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_is_explicit_by_rel = Window.partitionBy("artist_id", "is_explicit").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_bpm_prior_1 = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(-1, -1)
w_last_known_release_date_mont = Window.partitionBy(F.month("last_known_release_date"), "is_explicit").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_is_explicit_by_last_known_re = Window.partitionBy("is_explicit").orderBy("last_known_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_sum_1_pr = Window.orderBy(F.col("duration_sec_sum_1_prior").desc_nulls_first(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_duration_sec_sum_1_pr_2 = Window.orderBy(F.col("duration_sec_sum_1_prior").desc_nulls_first(), "id")
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))

recent_tracks = (
    df_tracks
    .filter(F.col("bpm").between(73, 156))
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
    recent_tracks
    .select(
        "*",
        F.sum("duration_sec").over(w_artist_id).alias("artist_id_sum_duration_sec"),
        F.last("release_date", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("last_known_release_date"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
        F.last("duration_sec").over(w_artist_id_is_explicit_by_rel).alias("last_duration_sec"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
        F.max("bpm").over(w_artist_id).alias("artist_id_max_bpm"),
        F.rank().over(w_artist_id_by_bpm_ties).alias("bpm_rank"),
        F.max("duration_sec").over(w_artist_id_is_explicit_by_bpm).alias("max_duration_sec_so_far"),
        F.sum("duration_sec").over(w_artist_id_by_bpm_prior_1).alias("duration_sec_sum_1_prior"),
    )
)

leaders = (
    sequenced
    .filter(F.col("bpm_rank") <= 2)
    .drop("bpm_rank")
)

sequenced_2 = (
    leaders
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "artist_id_sum_duration_sec",
        "last_known_release_date",
        "last_duration_sec",
        "rows_in_artist_id",
        "max_duration_sec_so_far",
        "duration_sec_sum_1_prior",
        F.round(F.avg("duration_sec_sum_1_prior").over(w_last_known_release_date_mont), 2).alias("duration_sec_sum_1_prior_avg_6_prior"),
        F.first("duration_sec_sum_1_prior").over(w_is_explicit_by_last_known_re).alias("first_duration_sec_sum_1_prior"),
        F.max("rows_in_is_explicit").over(w_all_by_duration_sec_sum_1_pr).alias("rows_in_is_explicit_max_3"),
        F.lag("rows_in_artist_id").over(w_all_by_duration_sec_sum_1_pr_2).alias("prev_rows_in_artist_id"),
        F.max("last_duration_sec").over(w_release_date_month).alias("release_date_month_max_last_duration_sec"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
    )
)

result = (
    sequenced_2
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "artist_id_sum_duration_sec",
        "last_known_release_date",
        "rows_in_artist_id",
        "duration_sec_sum_1_prior",
        "first_duration_sec_sum_1_prior",
        "rows_in_is_explicit_max_3",
        "release_date_month_max_last_duration_sec",
        "distinct_artist_id_in_release_date_month",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
    .orderBy(F.col("title").asc())
)
