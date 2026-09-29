from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_genre = Window.partitionBy("artist_genre")
w_artist_genre_by_bpm = Window.partitionBy("artist_genre").orderBy("bpm", "id")
w_all_by_bpm = Window.orderBy("bpm", "id")
w_all_by_bpm_full = Window.orderBy("bpm", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_prior_2 = Window.orderBy("bpm", "id").rowsBetween(-2, -1)
w_all_by_bpm_share_of_artist_g = Window.orderBy(F.col("bpm_share_of_artist_genre").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_bpm_share_of_artist_g_2 = Window.orderBy(F.col("bpm_share_of_artist_genre").desc(), "id").rowsBetween(-3, Window.currentRow)
w_all_by_bpm_share_of_artist_g_3 = Window.orderBy(F.col("bpm_share_of_artist_genre").desc(), "id")

recent_tracks = (
    df_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

combined_details = (
    recent_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.bpm",
        "s.is_explicit_label",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

windowed = (
    combined_details
    .select(
        "*",
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_genre), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_genre"),
        F.ntile(5).over(w_artist_genre_by_bpm).alias("bpm_quintile"),
        F.lag("artist_debut_year").over(w_all_by_bpm).alias("prev_artist_debut_year"),
        F.first("artist_debut_year").over(w_all_by_bpm_full).alias("first_artist_debut_year"),
        F.lag("artist_debut_year").over(w_artist_genre_by_bpm).alias("prev_artist_debut_year_2"),
        F.round(F.cume_dist().over(w_all_by_bpm), 3).alias("bpm_cume_dist"),
        F.round(F.avg("artist_debut_year").over(w_all_by_bpm_prior_2), 2).alias("artist_debut_year_avg_2_prior"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_genre), F.lit(0)), 4).alias("bpm_share_of_artist_genre"),
    )
)

scored = (
    windowed
    .filter(F.col("bpm_quintile") < 8)
    .select(
        "*",
        F.concat(F.col("artist_genre"), F.lit("#"), F.col("id").cast("string")).alias("artist_genre_ref"),
        F.coalesce(F.col("artist_debut_year"), F.lit(0)).alias("artist_debut_year_filled"),
        F.coalesce(F.col("artist_country"), F.lit("UNKNOWN")).alias("artist_country_or_unknown"),
    )
)

with_trends = (
    scored
    .select(
        "*",
        F.nth_value("first_artist_debut_year", 2).over(w_all_by_bpm_share_of_artist_g).alias("second_first_artist_debut_year"),
        F.count(F.lit(1)).over(w_artist_genre).alias("rows_in_artist_genre"),
        F.sum("first_artist_debut_year").over(w_all_by_bpm_share_of_artist_g_2).alias("first_artist_debut_year_sum_4"),
        F.round(F.avg("first_artist_debut_year").over(w_artist_genre), 2).alias("artist_genre_avg_first_artist_debut_year"),
        F.dense_rank().over(w_all_by_bpm_share_of_artist_g_3).alias("bpm_share_of_artist_genre_dense_rank"),
    )
)

rollup_base = (
    with_trends
    .groupBy("artist_id", "bpm_quintile", "bpm_share_of_artist_genre_dense_rank")
    .agg(
        F.count(F.when(F.lower(F.col("title")).like("%t %"), 1)).alias("n_lower_title_like_t"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
        F.sum("artist_debut_year_filled").alias("sum_artist_debut_year_filled_check"),
    )
    .filter(F.col("sum_artist_debut_year_filled_check") > 2002)
    .drop("sum_artist_debut_year_filled_check")
)

result = (
    rollup_base
    .select(
        "artist_id",
        "bpm_quintile",
        "bpm_share_of_artist_genre_dense_rank",
        "n_lower_title_like_t",
        "artist_genre_list",
        F.array_join(F.array_sort(F.col("artist_genre_list")), ",").alias("artist_genre_list_text"),
    )
    .orderBy(F.col("bpm_share_of_artist_genre_dense_rank").desc())
)
