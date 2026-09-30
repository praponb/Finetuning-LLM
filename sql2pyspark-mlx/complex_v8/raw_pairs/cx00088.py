from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter(F.col("artist_id").isNotNull())
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.array_contains(F.col("moods"), "focus").alias("has_focus"),
        F.when(F.col("duration_sec") >= 309, "A").when(F.col("duration_sec") >= 154, "B").otherwise("C").alias("duration_sec_tier"),
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)

combined_details = (
    base_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select("s.*", F.col("a.genre").alias("artist_genre"))
)

joined = (
    combined_details.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.lower(F.col("a.artist_name")).like("%it%"))
    .select("s.*", F.col("a.country").alias("artist_country"), "a.artist_name")
)

result = (
    joined
    .groupBy("is_explicit_label")
    .pivot("artist_genre", ["Indie", "HipHop"])
    .agg(F.sum("duration_sec"))
    .orderBy("is_explicit_label")
)
