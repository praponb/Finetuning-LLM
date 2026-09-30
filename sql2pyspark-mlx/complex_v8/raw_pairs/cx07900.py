from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter(F.col("bpm") >= 76)
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

enriched = (
    filtered
    .filter(F.array_contains(F.col("moods"), "chill"))
    .select(
        "*",
        F.abs(F.col("bpm") - 143).alias("bpm_distance"),
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        F.concat(F.col("is_explicit_label"), F.lit("#"), F.col("artist_id").cast("string")).alias("is_explicit_label_ref"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.length(F.trim(F.col("is_explicit_label_ref"))).alias("is_explicit_label_ref_len"),
        (F.floor(F.col("bpm_distance") / 25) * 25).alias("bpm_distance_bucket"),
    )
)

artist_id_stats = (
    labeled
    .groupBy("artist_id")
    .agg(F.avg("bpm_distance_bucket").alias("avg_bpm_distance_bucket"))
)

above_average = (
    labeled.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm_distance_bucket") > F.col("g.avg_bpm_distance_bucket"))
    .select(
        "s.*",
        "g.avg_bpm_distance_bucket",
        F.round(F.col("s.bpm_distance_bucket") - F.col("g.avg_bpm_distance_bucket"), 2).alias("bpm_distance_bucket_minus_avg"),
    )
)

result = (
    above_average
    .select("id", "bpm_distance_bucket_minus_avg", "avg_bpm_distance_bucket")
    .unpivot(
        ids=["id"],
        values=["bpm_distance_bucket_minus_avg", "avg_bpm_distance_bucket"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
    .orderBy("id", "measure")
)
