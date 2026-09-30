from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_artist_id_by_days_since_rele = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_month_artist_is = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date")
w_release_date_month_artist_is_2 = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_days_since_rele_2 = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id").rowsBetween(-6, Window.currentRow)
w_all_by_bpm_moving_2 = Window.orderBy("bpm", "id").rowsBetween(-2, Window.currentRow)
w_artist_id_by_days_since_rele_3 = Window.partitionBy("artist_id").orderBy("days_since_release_date")
w_artist_id_by_days_since_rele_4 = Window.partitionBy("artist_id").orderBy("days_since_release_date", "id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_release_date_month_artist_is_3 = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id").rowsBetween(-3, -1)
w_release_date_month_artist_is_4 = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy("release_date", "id")

recent = (
    df_tracks
    .filter((F.col("bpm") < 155) | (F.array_contains(F.col("moods"), "sad")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_parent_artist = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

metrics = (
    with_parent_artist
    .select(
        "*",
        F.min("duration_sec").over(w_artist_id).alias("artist_id_min_duration_sec"),
        F.last("release_date_month", ignorenulls=True).over(w_artist_id_by_days_since_rele).alias("last_known_release_date_month"),
        F.round(F.cume_dist().over(w_release_date_month_artist_is), 3).alias("release_date_cume_dist"),
        F.min("days_since_release_date").over(w_release_date_month_artist_is_2).alias("min_days_since_release_date_so_far"),
        F.max("bpm").over(w_artist_id_by_days_since_rele_2).alias("bpm_max_7"),
        F.max("duration_sec").over(w_all_by_bpm_moving_2).alias("duration_sec_max_3"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.dense_rank().over(w_artist_id_by_days_since_rele_3).alias("days_since_release_date_dense_rank"),
        F.row_number().over(w_artist_id_by_days_since_rele_4).alias("rn"),
        F.sum("days_since_release_date").over(w_artist_id).alias("artist_id_sum_days_since_release_date"),
        F.ntile(5).over(w_all_by_bpm).alias("bpm_quintile"),
        F.rank().over(w_artist_id_by_days_since_rele_3).alias("days_since_release_date_rank"),
        F.sum("artist_debut_year").over(w_release_date_month_artist_is_3).alias("artist_debut_year_sum_3_prior"),
        F.round(F.cume_dist().over(w_release_date_month_artist_is_4), 3).alias("release_date_cume_dist_2"),
        F.min("duration_sec").over(w_artist_id_by_days_since_rele).alias("min_duration_sec_so_far"),
    )
)

result = (
    metrics
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "artist_debut_year",
        "artist_is_verified",
        "artist_id_min_duration_sec",
        "last_known_release_date_month",
        "min_days_since_release_date_so_far",
        "bpm_max_7",
        "duration_sec_max_3",
        "bpm_share_of_artist_id",
        "days_since_release_date_dense_rank",
        "rn",
        "bpm_quintile",
        "artist_debut_year_sum_3_prior",
        "min_duration_sec_so_far",
        F.coalesce(F.col("bpm_max_7"), F.lit(0)).alias("bpm_max_7_filled"),
    )
    .orderBy(
        F.col("bpm_max_7_filled").desc(),
        F.col("min_days_since_release_date_so_far").desc(),
    )
)
