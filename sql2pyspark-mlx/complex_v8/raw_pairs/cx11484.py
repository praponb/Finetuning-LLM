from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("release_date").isNotNull()) & (F.upper(F.trim(F.col("title"))).like("R%")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

annotated = (
    base_tracks
    .filter(F.col("bpm") > 76)
    .select(
        "*",
        F.when(F.col("bpm") >= 147, "A").when(F.col("bpm") >= 96, "B").otherwise("C").alias("bpm_tier"),
        F.coalesce(F.col("bpm"), F.lit(0)).alias("bpm_filled"),
        F.add_months(F.col("release_date"), 12).alias("release_date_plus_12m"),
    )
)

with_details = (
    annotated.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select("s.*", F.col("a.country").alias("artist_country"), "a.artist_name")
)

per_group = (
    with_details
    .groupBy("artist_id", "artist_country")
    .agg(
        F.round(F.avg("bpm"), 2).alias("avg_bpm"),
        F.min("release_date_plus_12m").alias("first_release_date_plus_12m"),
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
        F.sum(F.when(F.col("artist_country") != "US", F.col("bpm")).otherwise(0)).alias("bpm_artist_country_us"),
        F.count(F.when(F.upper(F.trim(F.col("artist_name"))).like("S%"), 1)).alias("n_upper_trim_artist_name_like"),
        F.max("release_date_plus_12m").alias("last_release_date_plus_12m"),
        F.avg("bpm_filled").alias("avg_bpm_filled_check"),
    )
    .filter(F.col("avg_bpm_filled_check") > 78)
    .drop("avg_bpm_filled_check")
)

result = (
    per_group
    .select(
        "artist_id",
        "artist_country",
        F.col("stddev_bpm").alias("m_stddev_bpm"),
        F.col("avg_bpm").alias("avg_bpm_v"),
    )
    .unpivot(
        ids=["artist_id", "artist_country"],
        values=["m_stddev_bpm", "avg_bpm_v"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("artist_id", "artist_country", "measure")
)
