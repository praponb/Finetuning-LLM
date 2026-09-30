from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id")
w_artist_is_verified_by_days_s = Window.partitionBy("artist_is_verified").orderBy(F.col("days_since_release_date").desc(), "id")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_artist_is_verified_artist_id_2 = Window.partitionBy("artist_is_verified", "artist_id").orderBy("days_since_release_date", "id")
w_artist_is_verified_by_days_s_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("days_since_release_date").desc())
w_all_by_days_since_release_da = Window.orderBy(F.col("days_since_release_date_share_of_artist_is_verif").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_days_since_release_da_2 = Window.orderBy(F.col("days_since_release_date_share_of_artist_is_verif").desc(), "id").rowsBetween(-6, -1)
w_all_by_days_since_release_da_3 = Window.orderBy(F.col("days_since_release_date_share_of_artist_is_verif").desc(), "id")

scoped_tracks = (
    df_tracks
    .filter((F.col("bpm").between(89, 170)) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "moods",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

combined_details_artist = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.days_since_release_date",
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    combined_details_artist
    .select(
        "*",
        F.count(F.lit(1)).over(w_artist_is_verified_artist_id).alias("rows_in_artist_is_verified_artist_id"),
        F.lag("days_since_release_date", 3, 0).over(w_artist_is_verified_by_days_s).alias("prev3_days_since_release_date"),
        F.min("days_since_release_date").over(w_artist_is_verified).alias("artist_is_verified_min_days_since_release_date"),
        F.max("days_since_release_date").over(w_artist_is_verified_artist_id).alias("artist_is_verified_artist_id_max_days_since_rele"),
        F.lag("days_since_release_date", 1, 0).over(w_artist_is_verified_artist_id_2).alias("prev_days_since_release_date"),
        F.round(F.col("days_since_release_date") / F.nullif(F.sum("days_since_release_date").over(w_artist_is_verified_artist_id), F.lit(0)), 4).alias("days_since_release_date_share_of_artist_is_verif"),
        F.round(F.percent_rank().over(w_artist_is_verified_by_days_s_2), 3).alias("days_since_release_date_percent_rank"),
    )
)

windowed = (
    with_trends
    .select(
        "id",
        "moods",
        "days_since_release_date",
        "artist_is_verified",
        "rows_in_artist_is_verified_artist_id",
        "artist_is_verified_min_days_since_release_date",
        "artist_is_verified_artist_id_max_days_since_rele",
        "days_since_release_date_share_of_artist_is_verif",
        "days_since_release_date_percent_rank",
        F.first("days_since_release_date").over(w_all_by_days_since_release_da).alias("first_days_since_release_date"),
        F.sum("prev_days_since_release_date").over(w_all_by_days_since_release_da_2).alias("prev_days_since_release_date_sum_6_prior"),
        F.round(F.col("days_since_release_date") - F.avg("days_since_release_date").over(w_artist_is_verified_artist_id), 2).alias("days_since_release_date_vs_artist_is_verified_ar"),
        F.round(F.avg("days_since_release_date").over(w_artist_is_verified_artist_id), 2).alias("artist_is_verified_artist_id_avg_days_since_rele"),
        F.row_number().over(w_all_by_days_since_release_da_3).alias("days_since_release_date_share_of_artist_is_verif_2"),
    )
)

result = (
    windowed
    .select(
        "id",
        "moods",
        "days_since_release_date",
        "artist_is_verified",
        "rows_in_artist_is_verified_artist_id",
        "artist_is_verified_min_days_since_release_date",
        "artist_is_verified_artist_id_max_days_since_rele",
        "days_since_release_date_share_of_artist_is_verif",
        "days_since_release_date_percent_rank",
        "prev_days_since_release_date_sum_6_prior",
        "days_since_release_date_vs_artist_is_verified_ar",
        "artist_is_verified_artist_id_avg_days_since_rele",
        "days_since_release_date_share_of_artist_is_verif_2",
        F.when(F.col("artist_is_verified"), "yes").otherwise("no").alias("artist_is_verified_label"),
        F.coalesce(F.col("days_since_release_date"), F.lit(0)).alias("days_since_release_date_filled"),
    )
    .orderBy(F.col("days_since_release_date_filled").desc())
)
