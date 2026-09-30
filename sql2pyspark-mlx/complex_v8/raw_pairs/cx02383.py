from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_release_date_week_run = Window.orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_is_explic = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_is_verified_is_explic_2 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("release_date_week", "id")
w_artist_is_verified_is_explic_3 = Window.partitionBy("artist_is_verified", "is_explicit").orderBy("release_date_week")
w_all_by_release_date_week = Window.orderBy("release_date_week", "id")
w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified").orderBy("release_date_week", "id")
w_all_by_release_date_week_ful = Window.orderBy("release_date_week", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

filtered_tracks = (
    df_tracks
    .filter((F.lower(F.col("title")).like("%ng%")) & (F.col("bpm") < 166))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.date_trunc("week", F.col("release_date")).alias("release_date_week"),
    )
)

enriched = (
    filtered_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.release_date_week",
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

windowed = (
    enriched
    .select(
        "*",
        F.last("release_date_week", ignorenulls=True).over(w_all_by_release_date_week_run).alias("last_known_release_date_week"),
        F.first("release_date_week", ignorenulls=True).over(w_artist_is_verified_is_explic).alias("first_known_release_date_week"),
        F.lead("release_date_week").over(w_artist_is_verified_is_explic_2).alias("next_release_date_week"),
        F.ntile(4).over(w_artist_is_verified_is_explic_2).alias("release_date_week_quartile"),
        F.row_number().over(w_artist_is_verified_is_explic_2).alias("release_date_week_seq"),
        F.rank().over(w_artist_is_verified_is_explic_3).alias("release_date_week_rank"),
        F.ntile(10).over(w_artist_is_verified_is_explic_2).alias("release_date_week_decile"),
        F.row_number().over(w_all_by_release_date_week).alias("seq_no"),
        F.round(F.cume_dist().over(w_artist_is_verified_is_explic_2), 3).alias("release_date_week_cume_dist"),
        F.lag("release_date_week", 2).over(w_is_explicit_artist_is_verifi).alias("prev2_release_date_week"),
        F.ntile(10).over(w_all_by_release_date_week).alias("release_date_week_decile_2"),
        F.rank().over(w_artist_is_verified_is_explic_2).alias("release_date_week_rank_2"),
        F.nth_value("release_date_week", 2).over(w_all_by_release_date_week_ful).alias("second_release_date_week"),
    )
)

result = (
    windowed
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "release_date_week",
        "artist_is_verified",
        "last_known_release_date_week",
        "first_known_release_date_week",
        "release_date_week_quartile",
        "release_date_week_rank",
        "seq_no",
        "release_date_week_cume_dist",
        "prev2_release_date_week",
        "release_date_week_decile_2",
        "release_date_week_rank_2",
        "second_release_date_week",
        F.concat(F.col("title"), F.lit("#"), F.col("release_date_week_rank_2").cast("string")).alias("title_ref"),
    )
    .orderBy(
        "first_known_release_date_week",
        F.col("second_release_date_week").desc(),
    )
)
