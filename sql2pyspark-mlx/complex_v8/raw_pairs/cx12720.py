from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_release_date_month_by_releas = Window.partitionBy(F.date_trunc("month", "release_date")).orderBy(F.col("release_date").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_is_explicit = Window.partitionBy("is_explicit")
w_artist_id_by_release_date_2 = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id")
w_is_explicit_release_date_yea = Window.partitionBy("is_explicit", F.year("release_date"))
w_all_by_bpm_full = Window.orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_release_date_fu = Window.partitionBy("artist_id").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_running = Window.orderBy(F.col("bpm").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_artist_id_by_release_date_pr = Window.partitionBy("artist_id").orderBy(F.col("release_date").desc(), "id").rowsBetween(-1, -1)
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").asc_nulls_last())

filtered = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
)

with_trends = (
    filtered
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "title_ref",
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_release_date)).alias("days_since_prev_release_date"),
        F.lead("release_date", 2).over(w_artist_id_by_bpm).alias("next2_release_date"),
        F.round(F.avg("bpm").over(w_release_date_month_by_releas), 2).alias("bpm_avg_3_prior"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit), F.lit(0)), 4).alias("bpm_share_of_is_explicit"),
        F.lag("bpm").over(w_artist_id_by_release_date_2).alias("prev_bpm"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_release_date_yea)).alias("distinct_artist_id_in_is_explicit_release_date_y"),
        F.last("bpm").over(w_all_by_bpm_full).alias("last_bpm"),
        F.first("release_date").over(w_artist_id_by_bpm_full).alias("first_release_date"),
        F.last("release_date").over(w_artist_id_by_release_date_fu).alias("last_release_date"),
        F.lag("bpm", 2).over(w_artist_id_by_release_date_2).alias("prev2_bpm"),
        F.count(F.lit(1)).over(w_all_by_bpm_running).alias("running_count"),
        F.max("bpm").over(w_artist_id_is_explicit).alias("artist_id_is_explicit_max_bpm"),
        F.sum("bpm").over(w_artist_id_by_release_date_pr).alias("bpm_sum_1_prior"),
        F.round(F.percent_rank().over(w_all_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.sum("bpm").over(w_release_date_month_by_releas).alias("bpm_sum_3_prior"),
    )
)

per_group = (
    with_trends
    .groupBy(
        F.date_format("next2_release_date", "yyyy-MM").alias("next2_release_date_ym"),
        F.date_format("last_release_date", "yyyy-MM").alias("last_release_date_ym"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("cnt"),
        F.sum("last_bpm").alias("total_last_bpm"),
        F.round(F.sum("last_bpm") / F.nullif(F.sum("running_count"), F.lit(0)), 3).alias("last_bpm_to_running_count_ratio"),
        F.avg("last_bpm").alias("avg_last_bpm_check"),
    )
    .filter(F.col("avg_last_bpm_check") > 76)
    .drop("avg_last_bpm_check")
)

result = (
    per_group
    .select(
        "next2_release_date_ym",
        "last_release_date_ym",
        "cnt",
        "total_last_bpm",
        "last_bpm_to_running_count_ratio",
        F.least(F.col("total_last_bpm"), F.col("cnt")).alias("least_total_last_bpm_cnt"),
    )
    .orderBy(F.col("last_bpm_to_running_count_ratio").desc())
)
