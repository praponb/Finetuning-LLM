from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter((F.col("duration_sec") > 144) & (F.array_contains(F.col("moods"), "chill")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "moods",
    )
)

flattened = (
    filtered_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        F.explode("moods").alias("mood"),
    )
)

result = (
    flattened
    .groupingSets(
        [["artist_id", "is_explicit"], ["artist_id"]],
        "artist_id", "is_explicit",
    )
    .agg(
        F.grouping("artist_id").alias("is_total_artist_id"),
        F.round(F.stddev("duration_sec"), 2).alias("stddev_duration_sec"),
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
