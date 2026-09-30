from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre_by_bpm = Window.partitionBy("artist_genre").orderBy(F.col("bpm").desc(), "id")
w_artist_id_by_bpm_running = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_releas = Window.partitionBy("artist_is_verified").orderBy("release_date", "id").rowsBetween(-3, Window.currentRow)
w_artist_id_by_bpm = Window.partitionBy("artist_id").orderBy("bpm", "id")
w_artist_id_by_bpm_full = Window.partitionBy("artist_id").orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_releas_2 = Window.partitionBy("artist_is_verified").orderBy("release_date", "id")
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_artist_is_verified_by_releas_3 = Window.partitionBy("artist_is_verified").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

joined_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    joined_artist
    .select(
        "*",
        F.lag("release_date", 2).over(w_artist_genre_by_bpm).alias("prev2_release_date"),
        F.min("bpm_bucket").over(w_artist_id_by_bpm_running).alias("min_bpm_bucket_so_far"),
        F.ntile(4).over(w_artist_genre_by_bpm).alias("bpm_quartile"),
        F.max("bpm_bucket").over(w_artist_is_verified_by_releas).alias("bpm_bucket_max_4"),
        F.ntile(4).over(w_artist_id_by_bpm).alias("bpm_quartile_2"),
        F.last("release_date").over(w_artist_id_by_bpm_full).alias("last_release_date"),
        F.lead("release_date").over(w_artist_is_verified_by_releas_2).alias("next_release_date"),
        (F.col("bpm_bucket") - F.lag("bpm_bucket").over(w_artist_is_verified_by_releas_2)).alias("bpm_bucket_change"),
        F.rank().over(w_artist_id_by_bpm).alias("bpm_rank"),
        F.round(F.col("bpm_bucket") / F.nullif(F.sum("bpm_bucket").over(w_artist_is_verified), F.lit(0)), 4).alias("bpm_bucket_share_of_artist_is_verified"),
        F.last("bpm_bucket", ignorenulls=True).over(w_artist_is_verified_by_releas_3).alias("last_known_bpm_bucket"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        "title_prefix",
        "bpm_bucket",
        "artist_genre",
        "prev2_release_date",
        "min_bpm_bucket_so_far",
        "bpm_bucket_max_4",
        "bpm_quartile_2",
        "last_release_date",
        "next_release_date",
        "bpm_bucket_change",
        "bpm_bucket_share_of_artist_is_verified",
        "last_known_bpm_bucket",
        F.coalesce(F.col("bpm_bucket"), F.lit(0)).alias("bpm_bucket_filled"),
    )
    .orderBy(
        "bpm_bucket_change",
        F.col("bpm_bucket_share_of_artist_is_verified").desc_nulls_first(),
    )
)
