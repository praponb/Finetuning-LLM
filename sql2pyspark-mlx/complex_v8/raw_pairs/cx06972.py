from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_bpm_moving_6 = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_all_by_release_date_moving_6 = Window.orderBy("release_date", "id").rowsBetween(-6, Window.currentRow)
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_is_explicit_artist_id_by_rel_2 = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_bpm_ties = Window.partitionBy("artist_id").orderBy("bpm")
w_all_by_running_count_running = Window.orderBy(F.col("running_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_is_explicit_by_bpm_cume_dist = Window.partitionBy("is_explicit").orderBy(F.col("bpm_cume_dist").desc(), "id")
w_is_explicit_by_bpm_cume_dist_2 = Window.partitionBy("is_explicit").orderBy(F.col("bpm_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month = Window.partitionBy(F.date_trunc("month", "release_date"))
w_all_by_running_count_full = Window.orderBy(F.col("running_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_tracks = (
    df_tracks
    .filter((F.col("is_explicit")) & (F.col("bpm") < 147))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

metrics = (
    cleaned_tracks
    .select(
        "*",
        F.round(F.avg("bpm").over(w_is_explicit_by_bpm_moving_6), 2).alias("bpm_avg_7"),
        F.last("bpm", ignorenulls=True).over(w_is_explicit_artist_id_by_rel).alias("last_known_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("bpm_share_of_artist_id_is_explicit"),
        F.round(F.avg("bpm").over(w_all_by_release_date_moving_6), 2).alias("bpm_avg_7_2"),
        F.count(F.lit(1)).over(w_is_explicit_by_bpm_running).alias("running_count"),
        F.ntile(4).over(w_is_explicit_by_bpm).alias("bpm_quartile"),
        F.first("bpm").over(w_is_explicit_artist_id_by_rel_2).alias("first_bpm"),
        F.row_number().over(w_artist_id_by_bpm).alias("rn"),
        F.count(F.lit(1)).over(w_artist_id_is_explicit).alias("rows_in_artist_id_is_explicit"),
        F.max("bpm").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_bpm"),
        F.round(F.cume_dist().over(w_artist_id_by_bpm_ties), 3).alias("bpm_cume_dist"),
    )
)

ranked = (
    metrics
    .select(
        "id",
        "release_date",
        "bpm",
        "bpm_avg_7",
        "bpm_avg_7_2",
        "bpm_quartile",
        "first_bpm",
        "rn",
        "artist_id_is_explicit_max_bpm",
        F.first("bpm", ignorenulls=True).over(w_all_by_running_count_running).alias("first_known_bpm"),
        F.lead("release_date").over(w_is_explicit_by_bpm_cume_dist).alias("next_release_date"),
        F.last("running_count").over(w_is_explicit_by_bpm_cume_dist_2).alias("last_running_count"),
        F.lag("bpm_avg_7", 3).over(w_is_explicit_by_bpm_cume_dist).alias("prev3_bpm_avg_7"),
        F.round(F.avg("bpm_avg_7").over(w_release_date_month), 2).alias("release_date_month_avg_bpm_avg_7"),
        F.first("bpm_quartile").over(w_all_by_running_count_full).alias("first_bpm_quartile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "release_date",
        "bpm",
        "bpm_avg_7",
        "bpm_avg_7_2",
        "bpm_quartile",
        "first_bpm",
        "artist_id_is_explicit_max_bpm",
        "first_known_bpm",
        "next_release_date",
        "prev3_bpm_avg_7",
        "release_date_month_avg_bpm_avg_7",
        (F.col("first_bpm_quartile") - F.col("bpm_avg_7_2")).alias("first_bpm_quartile_minus_bpm_avg_7_2"),
    )
)
