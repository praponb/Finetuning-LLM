from pyspark.sql import functions as F

valid_tracks = (
    df_tracks
    .filter((F.size(F.col("moods")) >= 1) & (F.length(F.trim(F.col("title"))) >= 6))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        F.length(F.trim(F.col("title"))).alias("title_len"),
    )
)

rollup_base = (
    valid_tracks
    .groupBy("title_len", "artist_id")
    .agg(
        F.sum("duration_sec").alias("total_duration_sec"),
        F.countDistinct("artist_id").alias("distinct_artist_ids"),
        F.avg("title_len").alias("avg_title_len_check"),
    )
    .filter(F.col("avg_title_len_check") > 3)
    .drop("avg_title_len_check")
)

tagged = (
    rollup_base
    .filter(F.col("distinct_artist_ids") > 2)
    .select(
        "*",
        F.greatest(F.col("title_len"), F.col("distinct_artist_ids")).alias("greatest_title_len_distinct_artist_ids"),
        F.when(F.col("distinct_artist_ids") >= 6, "HIGH").when(F.col("distinct_artist_ids") >= 4, "MEDIUM").otherwise("LOW").alias("distinct_artist_ids_tier"),
    )
)

screened = (
    tagged.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date").isNotNull()) & (F.col("t.is_explicit")), "left_anti")
)

with_parent = (
    screened.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (~F.col("a.is_verified")), "inner")
    .select("s.*", F.col("a.is_verified").alias("artist_is_verified"))
)

result = (
    with_parent
    .select(
        "title_len",
        "artist_id",
        "total_duration_sec",
        "distinct_artist_ids",
        "greatest_title_len_distinct_artist_ids",
        "distinct_artist_ids_tier",
        "artist_is_verified",
        (F.floor(F.col("title_len") / 2) * 2).alias("title_len_bucket"),
    )
    .orderBy("artist_id")
)
