from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_year_by_release = Window.partitionBy("release_date_year").orderBy("release_date", "id").rowsBetween(-2, -1)
w_release_date_year_by_release_2 = Window.partitionBy("release_date_year").orderBy("release_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_release_date_year_by_release_3 = Window.partitionBy("release_date_year").orderBy("release_date")
w_artist_id = Window.partitionBy("artist_id")
w_title_len_release_date_year = Window.partitionBy("title_len", "release_date_year").orderBy("release_date", "id").rowsBetween(-3, -1)
w_title_len_release_date_year_2 = Window.partitionBy("title_len", "release_date_year").orderBy("release_date", "id")
w_release_date_year = Window.partitionBy("release_date_year")
w_artist_id_by_artist_id_sum_r = Window.partitionBy("artist_id").orderBy("artist_id_sum_release_date_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_tracks
    .filter(F.col("release_date").between(F.lit("2024-05-12").cast("date"), F.lit("2025-08-07").cast("date")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.array_join(F.array_sort(F.col("moods")), ",").alias("moods_text"),
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

sequenced = (
    cleaned
    .select(
        "*",
        F.sum("title_len").over(w_release_date_year_by_release).alias("title_len_sum_2_prior"),
        F.first("release_date_year", ignorenulls=True).over(w_release_date_year_by_release_2).alias("first_known_release_date_year"),
        F.round(F.cume_dist().over(w_release_date_year_by_release_3), 3).alias("release_date_cume_dist"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "*",
        F.round(F.col("title_len_sum_2_prior") / F.nullif(F.sum("title_len_sum_2_prior").over(w_artist_id), F.lit(0)), 4).alias("title_len_sum_2_prior_share_of_artist_id"),
        F.sum("release_date_cume_dist").over(w_artist_id).alias("artist_id_sum_release_date_cume_dist"),
        F.max("bpm").over(w_title_len_release_date_year).alias("bpm_max_3_prior"),
        F.lag("release_date_cume_dist").over(w_title_len_release_date_year_2).alias("prev_release_date_cume_dist"),
        (F.col("bpm") - F.lag("bpm").over(w_title_len_release_date_year_2)).alias("bpm_change"),
        F.max("release_date_year").over(w_title_len_release_date_year).alias("release_date_year_max_3_prior"),
    )
)

windowed = (
    sequenced_2
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
        "moods",
        "moods_text",
        "title_len_sum_2_prior",
        "release_date_cume_dist",
        "title_len_sum_2_prior_share_of_artist_id",
        "artist_id_sum_release_date_cume_dist",
        "prev_release_date_cume_dist",
        "bpm_change",
        "release_date_year_max_3_prior",
        F.round(F.col("prev_release_date_cume_dist") - F.avg("prev_release_date_cume_dist").over(w_release_date_year), 2).alias("prev_release_date_cume_dist_vs_release_date_year"),
        F.last("bpm_change", ignorenulls=True).over(w_artist_id_by_artist_id_sum_r).alias("last_known_bpm_change"),
    )
)

with_details_artist = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
        "a.artist_name",
    )
)

tagged = (
    with_details_artist
    .select(
        "*",
        F.when(F.col("title_len_sum_2_prior") >= 36, "large").when(F.col("title_len_sum_2_prior") >= 10, "medium").otherwise("small").alias("title_len_sum_2_prior_tier"),
    )
)

result = (
    tagged
    .select(
        "id",
        "artist_id",
        "release_date",
        "moods",
        "moods_text",
        "title_len_sum_2_prior",
        "release_date_cume_dist",
        "artist_id_sum_release_date_cume_dist",
        "prev_release_date_cume_dist_vs_release_date_year",
        "last_known_bpm_change",
        "title_len_sum_2_prior_tier",
    )
    .orderBy(
        F.col("prev_release_date_cume_dist_vs_release_date_year").desc(),
        F.col("release_date").desc_nulls_first(),
    )
)
