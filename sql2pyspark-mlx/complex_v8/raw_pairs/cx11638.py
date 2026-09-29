from pyspark.sql import functions as F

base = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("N%"))
    .select(
        "id",
        "artist_id",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_add(F.col("release_date"), 7).alias("release_date_plus_7d"),
    )
)

with_activity = (
    base.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.device").isin("speaker", "car")), "left_semi")
    .filter(F.col("bpm") >= 94)
)

with_details = (
    with_activity.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

result = (
    with_details
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"], []],
        "is_explicit", "artist_id",
    )
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("n_is_explicit_false"),
        F.min("bpm").alias("min_bpm"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
