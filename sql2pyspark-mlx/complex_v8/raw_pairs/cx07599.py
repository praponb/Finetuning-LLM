from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month = Window.partitionBy(F.month("release_date"))
w_is_explicit_by_bpm_running = Window.partitionBy("is_explicit").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_gen = Window.partitionBy(F.year("release_date"), "artist_genre").orderBy(F.col("artist_debut_year").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_artist_gen_2 = Window.partitionBy(F.year("release_date"), "artist_genre").orderBy(F.col("artist_debut_year").desc(), "id")
w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_by_bpm = Window.partitionBy("is_explicit").orderBy("bpm", "id")
w_release_date_year_by_bpm_ful = Window.partitionBy(F.year("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_release_date_year_by_bpm_run = Window.partitionBy(F.year("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_by_bpm = Window.partitionBy(F.year("release_date")).orderBy(F.col("bpm").asc_nulls_last(), "id")
w_all_by_artist_debut_year_ful = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_bpm_rank_by_release_date_mon = Window.partitionBy("bpm_rank").orderBy("release_date_month_sum_bpm", "id").rowsBetween(-1, Window.currentRow)
w_first_known_release_date_yea = Window.partitionBy(F.year("first_known_release_date"))
w_all_by_first_known_release_d = Window.orderBy(F.col("first_known_release_date").asc_nulls_last())
w_bpm_rank_by_release_date_mon_2 = Window.partitionBy("bpm_rank").orderBy("release_date_month_sum_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_release_d_2 = Window.orderBy(F.col("first_known_release_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_release_d_3 = Window.orderBy(F.col("first_known_release_date").asc_nulls_last(), "id").rowsBetween(-6, -1)
w_row_count_bpm_rank = Window.partitionBy("row_count", "bpm_rank")
w_row_count_by_row_count_ties = Window.partitionBy("row_count").orderBy(F.col("row_count").desc())
w_row_count_by_row_count = Window.partitionBy("row_count").orderBy(F.col("row_count").desc(), "bpm_rank", "distinct_artist_id_in_release_date_month", "first_known_release_date_quarter_day")

cleaned = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) | (F.col("duration_sec").between(142, 392)))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

joined_artist = (
    cleaned.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.title_prefix",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    joined_artist
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "title_prefix",
        "artist_debut_year",
        F.sum("bpm").over(w_release_date_month).alias("release_date_month_sum_bpm"),
        F.max("bpm").over(w_is_explicit_by_bpm_running).alias("max_bpm_so_far"),
        F.first("bpm", ignorenulls=True).over(w_release_date_year_artist_gen).alias("first_known_bpm"),
        F.round(F.cume_dist().over(w_release_date_year_artist_gen_2), 3).alias("artist_debut_year_cume_dist"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_is_explicit_by_bpm)).alias("days_since_prev_release_date"),
        F.min("artist_debut_year").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_min_artist_debut_year"),
        F.nth_value("release_date", 2).over(w_release_date_year_by_bpm_ful).alias("second_release_date"),
        F.lag("bpm", 1, 0).over(w_all_by_artist_debut_year).alias("prev_bpm"),
        F.first("release_date", ignorenulls=True).over(w_release_date_year_by_bpm_run).alias("first_known_release_date"),
        F.size(F.collect_set("artist_id").over(w_release_date_month)).alias("distinct_artist_id_in_release_date_month"),
        F.rank().over(w_release_date_year_by_bpm).alias("bpm_rank"),
        F.sum("artist_debut_year").over(w_release_date_month).alias("release_date_month_sum_artist_debut_year"),
        F.round(F.cume_dist().over(w_all_by_artist_debut_year), 3).alias("artist_debut_year_cume_dist_2"),
        F.nth_value("artist_debut_year", 2).over(w_all_by_artist_debut_year_ful).alias("second_artist_debut_year"),
    )
)

scored = (
    windowed
    .select(
        "id",
        "artist_debut_year",
        "release_date_month_sum_bpm",
        "max_bpm_so_far",
        "first_known_bpm",
        "artist_debut_year_cume_dist",
        "days_since_prev_release_date",
        "is_explicit_artist_id_min_artist_debut_year",
        "second_release_date",
        "prev_bpm",
        "first_known_release_date",
        "distinct_artist_id_in_release_date_month",
        "bpm_rank",
        "release_date_month_sum_artist_debut_year",
        "artist_debut_year_cume_dist_2",
        "second_artist_debut_year",
        F.date_trunc("quarter", F.col("first_known_release_date")).alias("first_known_release_date_quarter"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.abs(F.col("max_bpm_so_far") - 1104).alias("max_bpm_so_far_distance"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.max("max_bpm_so_far").over(w_bpm_rank_by_release_date_mon).alias("max_bpm_so_far_max_2"),
        F.round(F.col("bpm_rank") - F.avg("bpm_rank").over(w_first_known_release_date_yea), 2).alias("bpm_rank_vs_first_known_release_date_year_avg"),
        F.min("max_bpm_so_far_distance").over(w_first_known_release_date_yea).alias("first_known_release_date_year_min_max_bpm_so_far"),
        F.round(F.cume_dist().over(w_all_by_first_known_release_d), 3).alias("first_known_release_date_cume_dist"),
        F.last("first_known_bpm").over(w_bpm_rank_by_release_date_mon_2).alias("last_first_known_bpm"),
        F.nth_value("bpm_rank", 2).over(w_all_by_first_known_release_d_2).alias("second_bpm_rank"),
        F.round(F.avg("max_bpm_so_far_distance").over(w_all_by_first_known_release_d_3), 2).alias("max_bpm_so_far_distance_avg_6_prior"),
    )
)

summary = (
    sequenced
    .groupBy(
        "bpm_rank",
        "distinct_artist_id_in_release_date_month",
        F.to_date("first_known_release_date_quarter").alias("first_known_release_date_quarter_day"),
    )
    .agg(
        F.round(F.stddev("last_first_known_bpm"), 2).alias("stddev_last_first_known_bpm"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("max_bpm_so_far_distance"), 2).alias("stddev_max_bpm_so_far_distance"),
        F.sum("release_date_month_sum_artist_debut_year").alias("total_release_date_month_sum_artist_debut_year"),
    )
)

metrics = (
    summary
    .select(
        "bpm_rank",
        "distinct_artist_id_in_release_date_month",
        "first_known_release_date_quarter_day",
        "stddev_last_first_known_bpm",
        "row_count",
        F.round(F.cume_dist().over(w_row_count_by_row_count_ties), 3).alias("row_count_cume_dist"),
        (F.col("bpm_rank") - F.lag("bpm_rank").over(w_row_count_by_row_count)).alias("bpm_rank_change"),
        F.lead("stddev_last_first_known_bpm", 3).over(w_row_count_by_row_count).alias("next3_stddev_last_first_known_bpm"),
    )
)

result = (
    metrics
    .select(
        "bpm_rank",
        "distinct_artist_id_in_release_date_month",
        "first_known_release_date_quarter_day",
        "stddev_last_first_known_bpm",
        "row_count",
        "row_count_cume_dist",
        "bpm_rank_change",
        "next3_stddev_last_first_known_bpm",
        F.coalesce(F.col("bpm_rank"), F.lit(0)).alias("bpm_rank_filled"),
    )
    .orderBy(F.col("bpm_rank_change").desc())
)
