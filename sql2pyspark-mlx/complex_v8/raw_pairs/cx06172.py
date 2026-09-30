from pyspark.sql import functions as F

filtered_tracks = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_trunc("year", F.col("release_date")).alias("release_date_year"),
    )
)

flattened = (
    filtered_tracks
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "release_date_year",
        F.posexplode_outer("moods").alias("mood_pos", "mood"),
    )
)

enriched = (
    flattened.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.genre").isNotNull()), "inner")
    .select(
        "s.*",
        F.col("a.is_verified").alias("artist_is_verified"),
        F.col("a.country").alias("artist_country"),
        "a.artist_name",
    )
)

enriched_2 = (
    enriched
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "release_date_year",
        "mood_pos",
        "mood",
        "artist_country",
        "artist_name",
        (F.col("mood_pos") - F.col("bpm")).alias("mood_pos_minus_bpm"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
        (F.floor(F.col("duration_sec") / 50) * 50).alias("duration_sec_bucket"),
    )
)

result = (
    enriched_2
    .select(
        "id",
        "mood_pos",
        "mood_pos_minus_bpm",
        "duration_sec_bucket",
    )
    .unpivot(
        ids=["id", "mood_pos"],
        values=["mood_pos_minus_bpm", "duration_sec_bucket"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .orderBy("id", "mood_pos", "measure")
)
