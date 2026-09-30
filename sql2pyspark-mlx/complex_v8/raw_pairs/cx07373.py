from pyspark.sql import functions as F

prepared = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_ref"),
    )
)

exploded = (
    prepared
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "title_ref",
        F.explode("moods").alias("mood"),
    )
)

with_activity = (
    exploded.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 82) & (F.year(F.col("t.release_date")) == 2025), "left_anti")
    .filter(F.col("is_explicit"))
)

result = (
    with_activity
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"], []],
        "is_explicit", "artist_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("title").isNotNull(), 1)) / F.count("*"), 1).alias("pct_title_is_not_null"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
