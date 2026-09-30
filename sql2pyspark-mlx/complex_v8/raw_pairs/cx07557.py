from pyspark.sql import functions as F

base = (
    df_tracks
    .filter((F.col("bpm") < 166) & (F.col("title").isNotNull()))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        F.upper(F.trim(F.col("title"))).alias("title_clean"),
        F.round(F.col("bpm") / F.nullif(F.col("duration_sec"), F.lit(0)), 3).alias("bpm_per_duration_sec"),
        F.concat(F.col("title"), F.lit("#"), F.col("id").cast("string")).alias("title_ref"),
    )
)

derived = (
    base
    .select(
        "*",
        F.length(F.trim(F.col("title_ref"))).alias("title_ref_len"),
        F.trunc(F.col("release_date"), "MM").alias("release_date_month_start"),
        F.concat_ws(" / ", F.col("title_clean"), F.col("title_ref")).alias("title_clean_title_ref_label"),
    )
)

without_matches = (
    derived
    .crossJoin(derived.agg(F.avg("duration_sec").alias("overall_avg_duration_sec")))
    .filter(F.col("duration_sec") > F.col("overall_avg_duration_sec"))
    .drop("overall_avg_duration_sec")
)

line_level_plays = (
    without_matches.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title_clean",
        "s.bpm_per_duration_sec",
        "s.title_ref",
        "s.release_date_month_start",
        "s.title_clean_title_ref_label",
        F.col("p.id").alias("play_id"),
        F.col("p.source").alias("play_source"),
        F.col("p.device").alias("play_device"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

with_details_artist = (
    line_level_plays.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .filter(F.col("a.genre").isin("HipHop", "Indie", "Jazz"))
    .select(
        "s.id",
        "s.artist_id",
        "s.title_clean",
        "s.bpm_per_duration_sec",
        "s.title_ref",
        "s.play_id",
        "s.play_source",
        "s.play_listener_id",
        F.col("a.genre").alias("artist_genre"),
    )
)

scored = (
    with_details_artist
    .filter(F.col("title_clean").isNotNull())
    .select(
        "id",
        "title_clean",
        "play_id",
        "play_listener_id",
        "artist_genre",
        F.length(F.trim(F.col("artist_genre"))).alias("artist_genre_len"),
        F.when(F.col("bpm_per_duration_sec") >= 7.5, "A").when(F.col("bpm_per_duration_sec") >= 4.0, "B").otherwise("C").alias("bpm_per_duration_sec_tier"),
        F.length(F.trim(F.col("title_ref"))).alias("title_ref_len"),
    )
)

result = (
    scored
    .filter(F.col("title_ref_len") <= 12)
    .select(
        "id",
        "title_clean",
        "play_id",
        "play_listener_id",
        "artist_genre",
        "bpm_per_duration_sec_tier",
        "title_ref_len",
        F.length(F.trim(F.col("title_clean"))).alias("title_clean_len"),
        F.coalesce(F.col("artist_genre"), F.lit("UNKNOWN")).alias("artist_genre_or_unknown"),
    )
    .orderBy(F.col("title_clean_len").asc(), "id", "play_id")
    .limit(20)
)
