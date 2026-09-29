from pyspark.sql import functions as F

prepared_tracks = (
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
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
        F.size(F.col("moods")).alias("moods_count"),
    )
)

with_details = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.greatest_bpm_duration_sec",
        "s.title_ref",
        "s.moods_count",
        "a.artist_name",
    )
)

result = (
    with_details
    .groupingSets(
        [["is_explicit", "artist_id"], ["is_explicit"]],
        "is_explicit", "artist_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.length(F.trim(F.col("artist_name"))) >= 5, 1)) / F.count("*"), 1).alias("pct_length_trim_artist_name_5"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
