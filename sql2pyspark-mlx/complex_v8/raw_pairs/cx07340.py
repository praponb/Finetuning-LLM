from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "moods",
        F.initcap(F.trim(F.col("title"))).alias("title_clean"),
    )
)

exploded = (
    prepared_tracks
    .select(
        "id",
        "artist_id",
        "release_date",
        "bpm",
        "title_clean",
        F.posexplode_outer("moods").alias("mood_pos", "mood"),
    )
)

result = (
    exploded
    .cube("mood", "artist_id")
    .agg(
        F.count("*").alias("n_rows"),
        F.percentile_approx("bpm", 0.5).alias("median_bpm"),
    )
    .orderBy(F.col("mood").asc_nulls_last(), F.col("artist_id").asc_nulls_last())
)
