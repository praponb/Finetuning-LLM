from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_is_verifi = Window.partitionBy("is_explicit", "artist_is_verified")
w_artist_is_verified_by_bpm = Window.partitionBy("artist_is_verified").orderBy(F.col("bpm").desc(), "id")
w_artist_id_artist_is_verified = Window.partitionBy("artist_id", "artist_is_verified")
w_artist_genre_by_release_date = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc(), "id")
w_artist_genre_by_release_date_2 = Window.partitionBy("artist_genre").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_is_verified_by_bpm_ru = Window.partitionBy("artist_is_verified").orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_artist_is_verified_by_bpm_pr = Window.partitionBy("artist_is_verified").orderBy(F.col("bpm").desc(), "id").rowsBetween(-6, -1)
w_release_date_month_artist_is = Window.partitionBy(F.month("release_date"), "artist_is_verified").orderBy(F.col("release_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_artist_id_by_rel = Window.partitionBy("is_explicit", "artist_id").orderBy(F.col("release_date").desc_nulls_first(), "id")

valid_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("G%")) & (F.year(F.col("release_date")) == 2024))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

combined_details_artist = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.genre").alias("artist_genre"),
        "a.artist_name",
    )
)

windowed = (
    combined_details_artist
    .select(
        "*",
        F.size(F.collect_set("artist_id").over(w_is_explicit_artist_is_verifi)).alias("distinct_artist_id_in_is_explicit_artist_is_veri"),
        F.ntile(3).over(w_artist_is_verified_by_bpm).alias("bpm_tercile"),
        F.max("duration_sec").over(w_artist_id_artist_is_verified).alias("artist_id_artist_is_verified_max_duration_sec"),
        F.lead("duration_sec").over(w_artist_genre_by_release_date).alias("next_duration_sec"),
        F.nth_value("release_date", 2).over(w_artist_genre_by_release_date_2).alias("second_release_date"),
        F.last("release_date", ignorenulls=True).over(w_artist_is_verified_by_bpm_ru).alias("last_known_release_date"),
        F.datediff(F.col("release_date"), F.lag("release_date").over(w_artist_id_by_release_date)).alias("days_since_prev_release_date"),
        F.round(F.avg("duration_sec").over(w_artist_is_verified_by_bpm_pr), 2).alias("duration_sec_avg_6_prior"),
        F.nth_value("bpm", 2).over(w_release_date_month_artist_is).alias("second_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_rel), 3).alias("release_date_percent_rank"),
        F.lead("bpm").over(w_artist_id_by_release_date).alias("next_bpm"),
        F.min("duration_sec").over(w_is_explicit_artist_is_verifi).alias("is_explicit_artist_is_verified_min_duration_sec"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_is_explicit_artist_is_verifi), F.lit(0)), 4).alias("bpm_share_of_is_explicit_artist_is_verified"),
        F.count(F.lit(1)).over(w_artist_id_artist_is_verified).alias("rows_in_artist_id_artist_is_verified"),
    )
)

labeled = (
    windowed
    .select(
        "*",
        F.greatest(F.col("next_duration_sec"), F.col("bpm")).alias("greatest_next_duration_sec_bpm"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

result = (
    labeled
    .filter(F.col("greatest_next_duration_sec_bpm") < 366)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        "artist_is_verified",
        "artist_name",
        "distinct_artist_id_in_is_explicit_artist_is_veri",
        "bpm_tercile",
        "artist_id_artist_is_verified_max_duration_sec",
        "next_duration_sec",
        "duration_sec_avg_6_prior",
        "release_date_percent_rank",
        "next_bpm",
        "is_explicit_artist_is_verified_min_duration_sec",
        "bpm_share_of_is_explicit_artist_is_verified",
        "rows_in_artist_id_artist_is_verified",
        "greatest_next_duration_sec_bpm",
        "title_len",
        F.when(F.col("artist_genre").isin("Indie", "HipHop"), "group_a").when(F.col("artist_genre") == "Classical", "group_b").otherwise("other").alias("artist_genre_group"),
    )
    .orderBy(F.col("greatest_next_duration_sec_bpm").asc_nulls_last())
)
