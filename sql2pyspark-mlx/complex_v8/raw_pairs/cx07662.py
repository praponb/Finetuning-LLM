from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_artist_id = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date", "id")
w_release_date_month_artist_id_2 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_month = Window.partitionBy("artist_id", F.month("release_date"))
w_all_by_release_date_moving_3 = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_release_date_running = Window.orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_id_3 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_release_date_month_artist_id_4 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy(F.col("release_date_month_end").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_release_date_month_artist_id_5 = Window.partitionBy(F.date_trunc("month", "release_date"), "artist_id").orderBy("release_date")

recent = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
        F.last_day(F.col("release_date")).alias("release_date_month_end"),
    )
)

metrics = (
    recent
    .select(
        "*",
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.datediff(F.col("release_date_month_end"), F.lag("release_date_month_end").over(w_release_date_month_artist_id)).alias("days_since_prev_release_date_month_end"),
        F.row_number().over(w_release_date_month_artist_id).alias("release_date_seq"),
        F.sum("bpm").over(w_release_date_month_artist_id_2).alias("running_bpm"),
        F.round(F.avg("bpm").over(w_artist_id), 2).alias("artist_id_avg_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_release_date_month), 2).alias("artist_id_release_date_month_avg_bpm"),
        F.max("bpm").over(w_all_by_release_date_moving_3).alias("bpm_max_4"),
        F.max("bpm").over(w_all_by_release_date_running).alias("max_bpm_so_far"),
        F.round(F.avg("bpm").over(w_release_date_month_artist_id_3), 2).alias("bpm_avg_7"),
        F.first("release_date").over(w_release_date_month_artist_id_4).alias("first_release_date"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_id_by_bpm)).alias("bpm_change"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_all_by_bpm)).alias("days_since_prev_release_date"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_release_date_month), F.lit(0)), 4).alias("bpm_share_of_artist_id_release_date_month"),
        F.rank().over(w_release_date_month_artist_id_5).alias("release_date_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "title_ref",
        "release_date_month_end",
        "bpm_share_of_artist_id",
        "days_since_prev_release_date_month_end",
        "release_date_seq",
        "artist_id_avg_bpm",
        "artist_id_release_date_month_avg_bpm",
        "bpm_max_4",
        "max_bpm_so_far",
        "first_release_date",
        "bpm_change",
        "days_since_prev_release_date",
        "bpm_share_of_artist_id_release_date_month",
        "release_date_rank",
        (F.col("artist_id_avg_bpm") - F.col("release_date_seq")).alias("artist_id_avg_bpm_minus_release_date_seq"),
        F.lower(F.regexp_replace(F.trim(F.col("title_ref")), " ", "_")).alias("title_ref_slug"),
    )
    .orderBy(F.col("days_since_prev_release_date_month_end").desc_nulls_first())
)
