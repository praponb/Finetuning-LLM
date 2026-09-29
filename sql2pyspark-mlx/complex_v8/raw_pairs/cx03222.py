from pyspark.sql import functions as F

valid = (
    df_tracks
    .filter(F.col("bpm") >= 73)
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
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

artist_id_stats = (
    valid
    .groupBy("artist_id")
    .agg(F.percentile_approx("bpm", 0.5).alias("median_bpm"))
)

outliers = (
    valid.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") >= F.col("g.median_bpm"))
    .select(
        "s.*",
        "g.median_bpm",
        F.round(F.col("s.bpm") - F.col("g.median_bpm"), 2).alias("bpm_minus_median"),
    )
)

result = (
    outliers
    .select("id", "duration_sec", "bpm")
    .unpivot(
        ids=["id"],
        values=["duration_sec", "bpm"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "metric_name")
)
