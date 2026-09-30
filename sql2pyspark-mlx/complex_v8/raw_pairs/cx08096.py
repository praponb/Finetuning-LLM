from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc(), "id")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc_nulls_first(), "id")
w_release_date_month_by_least = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("least_bpm_duration_sec").asc_nulls_last(), "id")
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_by_releas_2 = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").desc_nulls_first())
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_least_bpm_duration = Window.partitionBy("artist_id", "least_bpm_duration_sec_rank")
w_artist_id_least_bpm_duration_2 = Window.partitionBy("artist_id", "least_bpm_duration_sec_rank").orderBy("prev_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_least_bpm_duration_3 = Window.partitionBy("artist_id", "least_bpm_duration_sec_rank").orderBy("prev_bpm", "id")
w_artist_id_least_bpm_duration_4 = Window.partitionBy("artist_id", "least_bpm_duration_sec_rank").orderBy("prev_bpm")
w_least_bpm_duration_sec_rank = Window.partitionBy("least_bpm_duration_sec_rank", "artist_id").orderBy("release_date_percent_rank")

valid_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 1)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

metrics = (
    valid_tracks
    .select(
        "*",
        F.lag("bpm").over(w_artist_id_by_bpm).alias("prev_bpm"),
        F.lead("bpm", 3).over(w_all_by_duration_sec).alias("next3_bpm"),
        F.last("duration_sec").over(w_all_by_duration_sec_full).alias("last_duration_sec"),
        F.round(F.avg("least_bpm_duration_sec").over(w_artist_id), 2).alias("artist_id_avg_least_bpm_duration_sec"),
        F.round(F.percent_rank().over(w_release_date_month_by_releas), 3).alias("release_date_percent_rank"),
        F.rank().over(w_release_date_month_by_least).alias("least_bpm_duration_sec_rank"),
        F.first("release_date", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_release_date"),
        F.round(F.percent_rank().over(w_release_date_month_by_releas_2), 3).alias("release_date_percent_rank_2"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_bpm)).alias("bpm_change"),
        F.first("duration_sec", ignorenulls=True).over(w_artist_id_by_bpm_running).alias("first_known_duration_sec"),
        F.dense_rank().over(w_all_by_duration_sec).alias("duration_sec_dense_rank"),
    )
)

first_rows = (
    metrics
    .filter(F.col("duration_sec_dense_rank") == 1)
    .drop("duration_sec_dense_rank")
)

sequenced = (
    first_rows
    .select(
        "*",
        F.round(F.avg("prev_bpm").over(w_artist_id_least_bpm_duration), 2).alias("artist_id_least_bpm_duration_sec_rank_avg_prev_b"),
        F.first("prev_bpm", ignorenulls=True).over(w_artist_id_least_bpm_duration_2).alias("first_known_prev_bpm"),
        F.lag("next3_bpm").over(w_artist_id_least_bpm_duration_3).alias("prev_next3_bpm"),
        F.round(F.percent_rank().over(w_artist_id_least_bpm_duration_4), 3).alias("prev_bpm_percent_rank"),
        F.dense_rank().over(w_least_bpm_duration_sec_rank).alias("release_date_percent_rank_dense_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("bpm") <= 142)
    .select(
        "id",
        "release_date",
        "bpm",
        "title_slug",
        "least_bpm_duration_sec",
        "prev_bpm",
        "last_duration_sec",
        "release_date_percent_rank",
        "least_bpm_duration_sec_rank",
        "release_date_percent_rank_2",
        "bpm_change",
        "first_known_duration_sec",
        "artist_id_least_bpm_duration_sec_rank_avg_prev_b",
        "prev_next3_bpm",
        "prev_bpm_percent_rank",
        "release_date_percent_rank_dense_rank",
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
    .orderBy(
        F.col("prev_next3_bpm").desc(),
        F.col("release_date_percent_rank").desc(),
        "id",
    )
    .limit(5)
)
