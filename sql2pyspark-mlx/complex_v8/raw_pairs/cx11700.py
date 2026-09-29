from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.lower(F.col("title")).like("%ap%"))
    .select(
        "id",
        "artist_id",
        "title",
        "is_explicit",
        "bpm",
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

artist_id_stats = (
    base
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

vs_group = (
    base.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.title_prefix",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    joined
    .select(
        "id",
        "artist_id",
        "title_prefix",
        "artist_debut_year",
        F.upper(F.trim(F.col("title_prefix"))).alias("title_prefix_clean"),
        F.concat(F.col("title_prefix"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_prefix_ref"),
    )
    .orderBy(F.col("title_prefix").asc_nulls_last())
)
