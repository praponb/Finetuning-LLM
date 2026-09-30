from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id = Window.partitionBy("is_explicit", "artist_id")
w_is_explicit_artist_country_b = Window.partitionBy("is_explicit", "artist_country").orderBy("release_date_month", "id")
w_artist_country_by_release_da = Window.partitionBy("artist_country").orderBy("release_date_month")
w_artist_country_by_release_da_2 = Window.partitionBy("artist_country").orderBy("release_date_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date_mo = Window.partitionBy("artist_id").orderBy("release_date_month", "id").rowsBetween(-3, -1)
w_all_by_minutes_since_prev_re = Window.orderBy("minutes_since_prev_release_date_month", "id").rowsBetween(-1, -1)
w_is_explicit_release_date_mon = Window.partitionBy("is_explicit", F.to_date("release_date_month")).orderBy("is_explicit_artist_id_avg_bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified = Window.partitionBy("artist_is_verified")

recent = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("month", F.col("release_date")).alias("release_date_month"),
    )
)

with_details = (
    recent.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.is_verified"))
    .select(
        "s.*",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

metrics = (
    with_details
    .select(
        "*",
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("bpm_vs_is_explicit_artist_id_avg"),
        F.lag("release_date_month").over(w_is_explicit_artist_country_b).alias("prev_release_date_month"),
        F.count(F.lit(1)).over(w_is_explicit_artist_id).alias("rows_in_is_explicit_artist_id"),
        F.round(F.cume_dist().over(w_artist_country_by_release_da), 3).alias("release_date_month_cume_dist"),
        F.sum("bpm").over(w_artist_country_by_release_da_2).alias("running_bpm"),
        F.sum("bpm").over(w_is_explicit_artist_id).alias("is_explicit_artist_id_sum_bpm"),
        F.round(F.avg("bpm").over(w_artist_id_by_release_date_mo), 2).alias("bpm_avg_3_prior"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_id), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_id"),
        F.round((F.unix_timestamp("release_date_month") - F.unix_timestamp(F.lag("release_date_month").over(w_is_explicit_artist_country_b))) / 60, 1).alias("minutes_since_prev_release_date_month"),
        F.round(F.avg("bpm").over(w_is_explicit_artist_id), 2).alias("is_explicit_artist_id_avg_bpm"),
    )
)

derived = (
    metrics
    .filter(F.col("running_bpm") >= 495)
    .select(
        "*",
        F.when(F.col("artist_is_verified"), 1).otherwise(0).alias("artist_is_verified_flag"),
        F.coalesce(F.col("running_bpm"), F.lit(0)).alias("running_bpm_filled"),
    )
)

with_windows = (
    derived
    .select(
        "*",
        F.round(F.avg("bpm").over(w_all_by_minutes_since_prev_re), 2).alias("bpm_avg_1_prior"),
        F.last("prev_release_date_month").over(w_is_explicit_release_date_mon).alias("last_prev_release_date_month"),
        F.sum("release_date_month_cume_dist").over(w_artist_is_verified).alias("artist_is_verified_sum_release_date_month_cume_d"),
        F.size(F.collect_set("artist_country").over(w_artist_is_verified)).alias("distinct_artist_country_in_artist_is_verified"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "artist_id",
        "bpm",
        "artist_country",
        "artist_genre",
        "artist_is_verified",
        "bpm_vs_is_explicit_artist_id_avg",
        "prev_release_date_month",
        "rows_in_is_explicit_artist_id",
        "release_date_month_cume_dist",
        "running_bpm",
        "is_explicit_artist_id_sum_bpm",
        "bpm_avg_3_prior",
        "bpm_share_of_is_explicit_artist_id",
        "minutes_since_prev_release_date_month",
        "is_explicit_artist_id_avg_bpm",
        "artist_is_verified_flag",
        "running_bpm_filled",
        "bpm_avg_1_prior",
        "last_prev_release_date_month",
        "artist_is_verified_sum_release_date_month_cume_d",
        "distinct_artist_country_in_artist_is_verified",
    )
    .orderBy(F.col("running_bpm_filled").desc())
)
