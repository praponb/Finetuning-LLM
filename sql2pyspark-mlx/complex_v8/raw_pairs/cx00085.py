from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("M%")) | (F.col("bpm") < 170))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "MMM yyyy").alias("release_date_label"),
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.lower(F.trim(F.col("title"))).alias("title_clean_2"),
    )
)

screened = (
    prepared_tracks.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (~F.col("p.source").isin("search", "album")), "left_anti")
)

derived = (
    screened
    .filter(F.array_contains(F.col("moods"), "sad"))
    .select(
        "*",
        F.concat(F.col("title_clean_2"), F.lit("#"), F.col("artist_id").cast("string")).alias("title_clean_2_ref"),
        F.length(F.trim(F.col("title_clean_2"))).alias("title_clean_2_len"),
    )
)

result = (
    derived
    .groupingSets(
        [["title_clean_2_len", "is_explicit", "artist_id"], ["title_clean_2_len"]],
        "title_clean_2_len", "is_explicit", "artist_id",
    )
    .agg(
        F.grouping("title_clean_2_len").alias("is_total_title_clean_2_len"),
        F.sum("title_clean_2_len").alias("total_title_clean_2_len"),
        F.count("*").alias("n_rows"),
    )
    .orderBy(
        F.col("title_clean_2_len").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
