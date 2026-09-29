from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_release_date_year = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("release_date_week").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_release_date_year_2 = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("release_date_week").desc_nulls_first(), "id").rowsBetween(-6, -1)
w_all_by_artist_debut_year = Window.orderBy("artist_debut_year", "id")
w_release_date_week_month_arti = Window.partitionBy(F.month("release_date_week"), "artist_id")
w_bpm_tier_is_explicit_by_rele = Window.partitionBy("bpm_tier", "is_explicit").orderBy(F.col("release_date_week").desc(), "id").rowsBetween(-6, -1)
w_release_date_week_to_date_bp = Window.partitionBy(F.to_date("release_date_week"), "bpm_tier")
w_artist_id_release_date_year_3 = Window.partitionBy("artist_id", F.year("release_date")).orderBy(F.col("release_date_week").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_artist_debut_year_run = Window.orderBy("artist_debut_year", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_duration_sec_ties = Window.orderBy("duration_sec")
w_all_by_release_date_week_mov = Window.orderBy("release_date_week", "id").rowsBetween(-3, Window.currentRow)
w_is_explicit_bpm_tier = Window.partitionBy("is_explicit", "bpm_tier")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_known_release_d = Window.orderBy("first_known_release_date_week", "id")
w_all_by_release_date_week = Window.orderBy("release_date_week", "id")

filtered = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
        F.when(F.col("bpm") >= 156, "large").when(F.col("bpm") >= 76, "medium").otherwise("small").alias("bpm_tier"),
    )
)

with_details_artist = (
    filtered.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.debut_year").alias("artist_debut_year"),
        "a.artist_name",
    )
)

sequenced = (
    with_details_artist
    .select(
        "*",
        F.last("release_date_week", ignorenulls=True).over(w_artist_id_release_date_year).alias("last_known_release_date_week"),
        F.sum("bpm").over(w_artist_id_release_date_year_2).alias("bpm_sum_6_prior"),
        (F.col("artist_debut_year") - F.lag("artist_debut_year").over(w_all_by_artist_debut_year)).alias("artist_debut_year_change"),
        F.count(F.lit(1)).over(w_release_date_week_month_arti).alias("rows_in_release_date_week_month_artist_id"),
        F.sum("bpm").over(w_bpm_tier_is_explicit_by_rele).alias("bpm_sum_6_prior_2"),
        F.max("artist_debut_year").over(w_release_date_week_to_date_bp).alias("release_date_week_to_date_bpm_tier_max_artist_de"),
        F.last("artist_debut_year").over(w_artist_id_release_date_year_3).alias("last_artist_debut_year"),
        F.max("duration_sec").over(w_all_by_artist_debut_year_run).alias("max_duration_sec_so_far"),
        F.first("release_date_week", ignorenulls=True).over(w_artist_id_release_date_year).alias("first_known_release_date_week"),
        F.round(F.percent_rank().over(w_all_by_duration_sec_ties), 3).alias("duration_sec_percent_rank"),
    )
)

with_trends = (
    sequenced
    .select(
        "*",
        F.round(F.avg("max_duration_sec_so_far").over(w_all_by_release_date_week_mov), 2).alias("max_duration_sec_so_far_avg_4"),
        F.size(F.collect_set("artist_id").over(w_is_explicit_bpm_tier)).alias("distinct_artist_id_in_is_explicit_bpm_tier"),
        F.last("bpm_sum_6_prior_2").over(w_all_by_bpm_full).alias("last_bpm_sum_6_prior_2"),
        F.round((F.unix_timestamp("last_known_release_date_week") - F.unix_timestamp(F.lag("last_known_release_date_week").over(w_all_by_first_known_release_d))) / 60, 1).alias("minutes_since_prev_last_known_release_date_week"),
        (F.col("bpm") - F.lag("bpm").over(w_all_by_first_known_release_d)).alias("bpm_change"),
        F.first("duration_sec").over(w_all_by_bpm_full).alias("first_duration_sec"),
        F.round(F.cume_dist().over(w_all_by_release_date_week), 3).alias("release_date_week_cume_dist"),
    )
)

result = (
    with_trends
    .filter(F.col("release_date") < F.lit("2024-12-22").cast("date"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "release_date_week",
        "artist_is_verified",
        "artist_debut_year",
        "artist_name",
        "last_known_release_date_week",
        "artist_debut_year_change",
        "rows_in_release_date_week_month_artist_id",
        "last_artist_debut_year",
        "max_duration_sec_so_far",
        "first_known_release_date_week",
        "duration_sec_percent_rank",
        "max_duration_sec_so_far_avg_4",
        "distinct_artist_id_in_is_explicit_bpm_tier",
        "last_bpm_sum_6_prior_2",
        "bpm_change",
        "first_duration_sec",
        "release_date_week_cume_dist",
    )
    .orderBy(
        F.col("bpm").desc_nulls_first(),
        F.col("duration_sec_percent_rank").desc(),
    )
)
