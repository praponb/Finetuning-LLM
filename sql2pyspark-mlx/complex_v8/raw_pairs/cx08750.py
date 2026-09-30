from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_country_release_date = Window.partitionBy("artist_country", F.date_trunc("month", "release_date")).orderBy("bpm")
w_all_by_bpm_running = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_country_release_date_2 = Window.partitionBy("artist_country", F.date_trunc("month", "release_date")).orderBy("bpm", "id")
w_artist_country_release_date_3 = Window.partitionBy("artist_country", F.date_trunc("month", "release_date")).orderBy("bpm", "id").rowsBetween(-6, -1)
w_artist_country = Window.partitionBy("artist_country")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_genre_release_date_mo = Window.partitionBy("artist_genre", F.month("release_date")).orderBy(F.col("duration_sec_distance").asc_nulls_last(), "id")
w_all_by_bpm_ties = Window.orderBy(F.col("bpm").desc_nulls_first())

base = (
    df_tracks
    .filter((F.col("bpm").between(75, 166)) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.abs(F.col("duration_sec") - 183).alias("duration_sec_distance"),
    )
)

combined_details_artist = (
    base.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.title_len",
        "s.duration_sec_distance",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

windowed = (
    combined_details_artist
    .select(
        "*",
        F.rank().over(w_artist_country_release_date).alias("bpm_rank"),
        F.last("title_len", ignorenulls=True).over(w_all_by_bpm_running).alias("last_known_title_len"),
        (F.col("title_len") - F.lag("title_len").over(w_artist_country_release_date_2)).alias("title_len_change"),
        F.round(F.avg("duration_sec_distance").over(w_artist_country_release_date_3), 2).alias("duration_sec_distance_avg_6_prior"),
        F.min("title_len").over(w_all_by_bpm_running).alias("min_title_len_so_far"),
        F.round(F.avg("duration_sec_distance").over(w_artist_country), 2).alias("artist_country_avg_duration_sec_distance"),
        F.round(F.col("title_len") - F.avg("title_len").over(w_artist_country), 2).alias("title_len_vs_artist_country_avg"),
        F.first("title_len").over(w_all_by_bpm_full).alias("first_title_len"),
        F.size(F.collect_set("artist_id").over(w_artist_country)).alias("distinct_artist_id_in_artist_country"),
        (F.col("bpm") - F.lag("bpm").over(w_artist_genre_release_date_mo)).alias("bpm_change"),
        F.count(F.lit(1)).over(w_artist_country).alias("rows_in_artist_country"),
        F.round(F.percent_rank().over(w_all_by_bpm_ties), 3).alias("bpm_percent_rank"),
        F.rank().over(w_all_by_bpm_ties).alias("bpm_rank_2"),
        (F.col("duration_sec_distance") - F.lag("duration_sec_distance").over(w_artist_genre_release_date_mo)).alias("duration_sec_distance_change"),
    )
)

leaders = windowed.filter(F.col("bpm_rank") == 1)

result = (
    leaders
    .select("bpm_change", "min_title_len_so_far", "duration_sec_distance")
    .distinct()
    .orderBy(
        F.col("bpm_change").asc_nulls_last(),
        F.col("min_title_len_so_far").desc(),
    )
)
