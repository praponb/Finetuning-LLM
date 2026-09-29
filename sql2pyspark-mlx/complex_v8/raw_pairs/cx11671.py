from pyspark.sql import functions as F

recent = (
    df_tracks
    .filter(~F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "moods",
        F.when(F.col("duration_sec") >= 352, "large").when(F.col("duration_sec") >= 213, "medium").otherwise("small").alias("duration_sec_tier"),
        F.when(F.col("duration_sec") >= 344, "A").when(F.col("duration_sec") >= 183, "B").otherwise("C").alias("duration_sec_tier_2"),
    )
)

exploded = (
    recent
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "bpm",
        "duration_sec_tier",
        "duration_sec_tier_2",
        F.explode_outer("moods").alias("mood"),
    )
)

combined_details = (
    exploded.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.duration_sec_tier_2",
        "s.mood",
        F.col("a.genre").alias("artist_genre"),
    )
)

result = (
    combined_details
    .groupBy("mood", "artist_id")
    .pivot("duration_sec_tier_2", ["B", "C", "A"])
    .agg(F.sum("bpm"))
)
