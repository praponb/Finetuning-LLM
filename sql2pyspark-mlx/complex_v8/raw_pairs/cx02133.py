from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.upper(F.trim(F.col("title"))).like("P%")))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "bpm",
    )
)

derived = (
    base
    .select(
        "*",
        F.concat(F.col("title"), F.lit("#"), F.col("bpm").cast("string")).alias("title_ref"),
        F.length(F.trim(F.col("title"))).alias("title_len"),
        F.length(F.trim(F.col("title"))).alias("title_len_2"),
    )
)

enriched = (
    derived
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "title_ref",
        "title_len",
        "title_len_2",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.upper(F.trim(F.col("title_ref"))).alias("title_ref_clean"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.first_played_at",
    )
)

result = (
    with_counts
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "title_ref",
        "title_len",
        "title_clean",
        "title_ref_clean",
        "play_count",
        "first_played_at",
        F.concat_ws(" / ", F.col("title_clean"), F.col("title_ref_clean")).alias("title_clean_title_ref_clean_label"),
    )
)
