from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.col("bpm") < 143)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
    )
)

enriched = (
    base
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("title")), " ", "_")).alias("title_slug"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
        F.substring(F.trim(F.col("title")), 1, 4).alias("title_prefix"),
    )
)

scored = (
    enriched
    .select(
        "*",
        F.when(F.col("duration_sec_bucket") >= 354, "HIGH").when(F.col("duration_sec_bucket") >= 156, "MEDIUM").otherwise("LOW").alias("duration_sec_bucket_tier"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket_2"),
        (F.floor(F.col("duration_sec_bucket") / 50) * 50).alias("duration_sec_bucket_bucket"),
    )
)

per_group = (
    scored
    .groupBy(
        "artist_id",
        "is_explicit",
        F.when(F.col("duration_sec") >= 402, "A").when(F.col("duration_sec") >= 209, "B").otherwise("C").alias("duration_sec_tier"),
    )
    .agg(
        F.count(F.when(F.lower(F.col("title")).like("%ol%"), 1)).alias("n_lower_title_like_ol"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.count("*").alias("row_count"),
        F.sum("duration_sec_bucket_2").alias("sum_duration_sec_bucket_2_check"),
    )
    .filter(F.col("sum_duration_sec_bucket_2_check") > 206)
    .drop("sum_duration_sec_bucket_2_check")
)

exploded_detail_tracks = (
    per_group.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") < 392), "inner")
    .select(
        "s.artist_id",
        "s.is_explicit",
        "s.duration_sec_tier",
        "s.row_count",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.is_explicit").alias("track_is_explicit"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

derived = (
    exploded_detail_tracks
    .filter(F.col("row_count") >= 5)
    .select(
        "*",
        (F.floor(F.col("row_count") / 2) * 2).alias("row_count_bucket"),
        F.date_format(F.col("track_release_date"), "MMM yyyy").alias("track_release_date_label"),
        F.coalesce(F.col("track_duration_sec"), F.lit(0)).alias("track_duration_sec_filled"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("played_at").alias("last_played_at"))
)

with_counts = (
    derived.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", "p.last_played_at")
)

result = (
    with_counts
    .select(
        "artist_id",
        "is_explicit",
        "row_count",
        "track_release_date",
        "track_is_explicit",
        "track_duration_sec",
        "row_count_bucket",
        "track_release_date_label",
        "track_duration_sec_filled",
        F.concat(F.col("track_release_date_label"), F.lit("#"), F.col("track_id").cast("string")).alias("track_release_date_label_ref"),
        F.substring(F.trim(F.col("track_release_date_label")), 1, 4).alias("track_release_date_label_prefix"),
    )
)
