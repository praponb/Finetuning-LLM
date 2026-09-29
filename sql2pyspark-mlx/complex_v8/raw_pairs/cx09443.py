from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified_artist_id = Window.partitionBy("artist_is_verified", "artist_id").orderBy(F.col("duration_sec").asc_nulls_last())
w_bpm_tier_artist_id = Window.partitionBy("bpm_tier", "artist_id")
w_bpm_tier_artist_id_by_durati = Window.partitionBy("bpm_tier", "artist_id").orderBy("duration_sec", "id").rowsBetween(-2, -1)
w_artist_genre = Window.partitionBy("artist_genre")
w_n_artist_is_verified_true_ar = Window.partitionBy("n_artist_is_verified_true", "artist_is_verified")
w_bpm_tier_by_distinct_artist = Window.partitionBy("bpm_tier").orderBy(F.col("distinct_artist_genres").desc(), "artist_is_verified", "artist_id", "bpm_tier")

scoped_tracks = (
    df_tracks
    .filter(F.size(F.col("moods")) >= 2)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.when(F.col("bpm") >= 142, "large").when(F.col("bpm") >= 100, "medium").otherwise("small").alias("bpm_tier"),
        (F.col("duration_sec") - F.col("bpm")).alias("duration_sec_minus_bpm"),
    )
)

with_parent = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.bpm_tier",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.genre").alias("artist_genre"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

with_trends = (
    with_parent
    .select(
        "*",
        F.rank().over(w_artist_is_verified_artist_id).alias("duration_sec_rank"),
        F.count(F.lit(1)).over(w_bpm_tier_artist_id).alias("rows_in_bpm_tier_artist_id"),
        F.min("artist_debut_year").over(w_bpm_tier_artist_id).alias("bpm_tier_artist_id_min_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_bpm_tier_artist_id_by_durati), 2).alias("artist_debut_year_avg_2_prior"),
        F.count(F.lit(1)).over(w_artist_genre).alias("rows_in_artist_genre"),
        F.sum("duration_sec").over(w_artist_genre).alias("artist_genre_sum_duration_sec"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_genre), 2).alias("duration_sec_vs_artist_genre_avg"),
    )
)

grouped = (
    with_trends
    .groupBy("artist_is_verified", "artist_id", "bpm_tier")
    .agg(
        F.countDistinct("artist_genre").alias("distinct_artist_genres"),
        F.percentile_approx("artist_debut_year", 0.5).alias("median_artist_debut_year"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
        F.count(F.when(F.col("artist_is_verified"), 1)).alias("n_artist_is_verified_true"),
        F.avg("bpm_tier_artist_id_min_artist_debut_year").alias("avg_bpm_tier_artist_id_min_artist_debut_year_che"),
    )
    .filter(F.col("avg_bpm_tier_artist_id_min_artist_debut_year_che") > 5638)
    .drop("avg_bpm_tier_artist_id_min_artist_debut_year_che")
)

ranked = (
    grouped
    .select(
        "artist_is_verified",
        "artist_id",
        "bpm_tier",
        "median_artist_debut_year",
        "artist_genre_list",
        F.size(F.collect_set("bpm_tier").over(w_n_artist_is_verified_true_ar)).alias("distinct_bpm_tier_in_n_artist_is_verified_true_a"),
        F.lag("median_artist_debut_year", 2).over(w_bpm_tier_by_distinct_artist).alias("prev2_median_artist_debut_year"),
        F.round(F.col("median_artist_debut_year") - F.avg("median_artist_debut_year").over(w_n_artist_is_verified_true_ar), 2).alias("median_artist_debut_year_vs_n_artist_is_verified"),
    )
)

result = (
    ranked
    .select(
        "artist_is_verified",
        "artist_genre_list",
        "prev2_median_artist_debut_year",
        "median_artist_debut_year_vs_n_artist_is_verified",
    )
    .orderBy(F.col("prev2_median_artist_debut_year").asc_nulls_last())
)
