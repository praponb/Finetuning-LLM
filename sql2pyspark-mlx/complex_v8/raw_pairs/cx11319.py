from pyspark.sql import functions as F

filtered = (
    df_tracks
    .filter((F.col("duration_sec") > 164) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

combined_details = (
    filtered.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.col("a.artist_name").isNotNull()), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.is_explicit",
        "s.moods",
        F.col("a.debut_year").alias("artist_debut_year"),
    )
)

screened = (
    combined_details.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.listener_id") <= 6) & (~F.col("p.is_skipped")), "left_anti")
    .filter(F.size(F.col("moods")) >= 1)
)

joined = (
    screened.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.is_explicit",
        "s.moods",
        "s.artist_debut_year",
        "a.artist_name",
    )
)

result = (
    joined
    .select(
        "id",
        "artist_id",
        "title",
        "moods",
        "artist_debut_year",
        "artist_name",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
    )
)
