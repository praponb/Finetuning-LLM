from pyspark.sql import functions as F

base_tracks = (
    df_tracks
    .filter((F.col("duration_sec") >= 209) & (~F.col("is_explicit")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
    )
)

artist_id_stats = (
    base_tracks
    .groupBy("artist_id")
    .agg(F.max("bpm").alias("max_bpm"))
)

vs_group = (
    base_tracks.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.bpm") == F.col("g.max_bpm"))
    .select(
        "s.*",
        "g.max_bpm",
        F.round(F.col("s.bpm") - F.col("g.max_bpm"), 2).alias("bpm_minus_max"),
    )
)

with_details_artist = (
    vs_group.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "s.max_bpm",
        "s.bpm_minus_max",
        "a.artist_name",
        F.col("a.debut_year").alias("artist_debut_year"),
        F.col("a.country").alias("artist_country"),
    )
)

screened = (
    with_details_artist.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec") >= 164), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["is_explicit", "artist_country", "artist_id"], ["is_explicit"], []],
        "is_explicit", "artist_country", "artist_id",
    )
    .agg(
        F.sum("artist_debut_year").alias("total_artist_debut_year"),
        F.sum(F.when(F.col("artist_country").isin("BR", "TH"), F.col("bpm"))).alias("bpm_artist_country_in_br_th"),
        F.count("*").alias("row_count"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("artist_country").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
