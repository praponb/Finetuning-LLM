from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter(F.col("bpm") >= 96)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        F.abs(F.col("bpm") - 118).alias("bpm_distance"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        (F.floor(F.col("bpm") / 25) * 25).alias("bpm_bucket"),
    )
)

labeled = (
    prepared
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm_distance",
        "duration_sec_bucket",
        "bpm_bucket",
        F.coalesce(F.col("duration_sec_bucket"), F.lit(0)).alias("duration_sec_bucket_filled"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
    )
)

joined = (
    labeled.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm_distance",
        "s.duration_sec_bucket",
        "s.bpm_bucket",
        "s.duration_sec_bucket_filled",
        "s.release_date_label",
        F.col("a.country").alias("artist_country"),
        F.col("a.genre").alias("artist_genre"),
    )
)

totals = (
    joined
    .groupBy("artist_country", "artist_id", "artist_genre")
    .agg(
        F.round(100 * F.count(F.when(F.col("is_explicit"), 1)) / F.count("*"), 1).alias("pct_is_explicit_true"),
        F.max("bpm_distance").alias("max_bpm_distance"),
        F.min("bpm_bucket").alias("min_bpm_bucket"),
        F.sum(F.when(~F.col("artist_country").isin("US", "KR"), 1).otherwise(0)).alias("n_artist_country_not_in_us_kr"),
        F.array_sort(F.collect_set("artist_genre")).alias("artist_genre_list"),
    )
)

enriched = (
    totals
    .select(
        "*",
        (F.floor(F.col("pct_is_explicit_true") / 25) * 25).alias("pct_is_explicit_true_bucket"),
    )
)

result = (
    enriched
    .select(
        "artist_country",
        "artist_id",
        "n_artist_country_not_in_us_kr",
        "max_bpm_distance",
    )
    .unpivot(
        ids=["artist_country", "artist_id"],
        values=["n_artist_country_not_in_us_kr", "max_bpm_distance"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("artist_country", "artist_id", "measure")
)
