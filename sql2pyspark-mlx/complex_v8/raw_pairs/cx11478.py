from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_is_verified = Window.partitionBy("artist_is_verified")
w_all_by_duration_sec_full = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_duration_sec_running = Window.orderBy(F.col("duration_sec").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_tracks
    .filter(F.col("bpm") < 142)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
    )
)

joined = (
    prepared.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.debut_year").between(1996, 2006))
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.moods",
        "s.title_slug",
        F.col("a.genre").alias("artist_genre"),
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

ranked = (
    joined
    .select(
        "*",
        F.max("artist_debut_year").over(w_artist_is_verified).alias("artist_is_verified_max_artist_debut_year"),
        F.round(F.avg("artist_debut_year").over(w_artist_is_verified), 2).alias("artist_is_verified_avg_artist_debut_year"),
        F.round(F.col("artist_debut_year") - F.avg("artist_debut_year").over(w_artist_is_verified), 2).alias("artist_debut_year_vs_artist_is_verified_avg"),
        F.first("artist_debut_year").over(w_all_by_duration_sec_full).alias("first_artist_debut_year"),
        F.max("artist_debut_year").over(w_all_by_duration_sec_running).alias("max_artist_debut_year_so_far"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_is_verified), 2).alias("duration_sec_vs_artist_is_verified_avg"),
        F.round(F.col("artist_debut_year") / F.nullif(F.sum("artist_debut_year").over(w_artist_is_verified), F.lit(0)), 4).alias("artist_debut_year_share_of_artist_is_verified"),
    )
)

annotated = (
    ranked
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "moods",
        "title_slug",
        "artist_debut_year",
        "artist_is_verified",
        "artist_is_verified_max_artist_debut_year",
        "artist_is_verified_avg_artist_debut_year",
        "artist_debut_year_vs_artist_is_verified_avg",
        "first_artist_debut_year",
        F.substring(F.trim(F.col("title_slug")), 1, 3).alias("title_slug_prefix"),
        F.least(F.col("duration_sec"), F.col("first_artist_debut_year")).alias("least_duration_sec_first_artist_debut_year"),
        F.substring(F.trim(F.col("title_slug")), 1, 2).alias("title_slug_prefix_2"),
    )
)

rollup_base = (
    annotated
    .groupBy("artist_id", "artist_is_verified")
    .agg(
        F.sum("first_artist_debut_year").alias("total_first_artist_debut_year"),
        F.percentile_approx("artist_debut_year", 0.5).alias("median_artist_debut_year"),
    )
)

result = (
    rollup_base
    .select(
        "artist_id",
        "median_artist_debut_year",
        F.when(F.col("median_artist_debut_year") >= 2010, "large").when(F.col("median_artist_debut_year") >= 2000, "medium").otherwise("small").alias("median_artist_debut_year_tier"),
        F.abs(F.col("median_artist_debut_year") - 2005).alias("median_artist_debut_year_distance"),
    )
    .orderBy(
        F.col("median_artist_debut_year_tier").desc(),
        F.col("median_artist_debut_year").desc(),
    )
)
