from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moods_count_artist_id_by_moo = Window.partitionBy("moods_count", "artist_id").orderBy(F.col("moods_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified").orderBy("moods_count")
w_artist_genre_by_moods_count = Window.partitionBy("artist_genre").orderBy(F.col("moods_count").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moods_count_artist_id_by_moo_2 = Window.partitionBy("moods_count", "artist_id").orderBy(F.col("moods_count").asc_nulls_last(), "id")
w_all_by_moods_count_prior_2 = Window.orderBy(F.col("moods_count").desc_nulls_first(), "id").rowsBetween(-2, -1)
w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_all_by_duration_sec_running = Window.orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_by_moods = Window.partitionBy("artist_is_verified").orderBy(F.col("moods_count_percent_rank").asc_nulls_last(), "id")
w_artist_is_verified_by_moods_2 = Window.partitionBy("artist_is_verified").orderBy(F.col("moods_count_percent_rank").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_genre_moods_count_ran = Window.partitionBy("artist_genre", "moods_count_rank")
w_second_moods_count_by_moods = Window.partitionBy("second_moods_count").orderBy(F.col("moods_count_percent_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = (
    df_tracks
    .filter(F.col("duration_sec").between(144, 357))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        F.when(F.col("duration_sec") >= 352, "HIGH").when(F.col("duration_sec") >= 164, "MEDIUM").otherwise("LOW").alias("duration_sec_tier"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_parent_artist = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.moods_count",
        "a.artist_name",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
    )
)

windowed = (
    with_parent_artist
    .select(
        "*",
        F.min("moods_count").over(w_moods_count_artist_id_by_moo).alias("min_moods_count_so_far"),
        F.rank().over(w_artist_id_artist_is_verified).alias("moods_count_rank"),
        F.nth_value("moods_count", 2).over(w_artist_genre_by_moods_count).alias("second_moods_count"),
        F.lag("duration_sec", 2).over(w_moods_count_artist_id_by_moo_2).alias("prev2_duration_sec"),
        F.max("moods_count").over(w_all_by_moods_count_prior_2).alias("moods_count_max_2_prior"),
        F.sum("moods_count").over(w_artist_is_verified).alias("artist_is_verified_sum_moods_count"),
        F.round(F.percent_rank().over(w_artist_id_artist_is_verified), 3).alias("moods_count_percent_rank"),
        F.first("duration_sec", ignorenulls=True).over(w_all_by_duration_sec_running).alias("first_known_duration_sec"),
        F.count(F.lit(1)).over(w_artist_is_verified).alias("rows_in_artist_is_verified"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.rank().over(w_artist_is_verified_by_moods).alias("moods_count_percent_rank_rank"),
        F.last("rows_in_artist_is_verified", ignorenulls=True).over(w_artist_is_verified_by_moods_2).alias("last_known_rows_in_artist_is_verified"),
        F.count(F.lit(1)).over(w_artist_genre_moods_count_ran).alias("rows_in_artist_genre_moods_count_rank"),
        F.round(F.col("second_moods_count") / F.nullif(F.sum("second_moods_count").over(w_artist_genre_moods_count_ran), F.lit(0)), 4).alias("second_moods_count_share_of_artist_genre_moods_c"),
        F.last("moods_count_rank").over(w_second_moods_count_by_moods).alias("last_moods_count_rank"),
    )
)

result = (
    sequenced
    .filter(~F.col("artist_is_verified"))
    .select(
        "id",
        "artist_id",
        "artist_name",
        "artist_is_verified",
        "artist_genre",
        "min_moods_count_so_far",
        "moods_count_rank",
        "second_moods_count",
        "prev2_duration_sec",
        "moods_count_max_2_prior",
        "artist_is_verified_sum_moods_count",
        "moods_count_percent_rank",
        "first_known_duration_sec",
        "rows_in_artist_is_verified",
        "moods_count_percent_rank_rank",
        "last_known_rows_in_artist_is_verified",
        "rows_in_artist_genre_moods_count_rank",
        "last_moods_count_rank",
        F.round(F.col("moods_count_rank") / F.nullif(F.col("moods_count_max_2_prior"), F.lit(0)), 3).alias("moods_count_rank_per_moods_count_max_2_prior"),
        F.coalesce(F.col("last_moods_count_rank"), F.lit(0)).alias("last_moods_count_rank_filled"),
    )
    .orderBy(F.col("artist_is_verified_sum_moods_count").desc())
)
