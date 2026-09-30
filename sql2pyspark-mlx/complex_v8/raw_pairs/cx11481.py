from pyspark.sql import functions as F

prepared_tracks = (
    df_tracks
    .filter((F.col("release_date") < F.lit("2025-03-29").cast("date")) & (F.size(F.col("moods")) >= 1))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.when(F.col("is_explicit"), "yes").otherwise("no").alias("is_explicit_label"),
        F.quarter(F.col("release_date")).alias("release_date_quarter"),
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
    )
)

enriched_artist = (
    prepared_tracks.alias("s")
    .join(df_artists.alias("a"), (F.col("a.id") == F.col("s.artist_id")) & (F.upper(F.trim(F.col("a.artist_name"))).like("S%")), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.is_explicit_label",
        "s.release_date_quarter",
        "s.release_date_label",
        "a.artist_name",
    )
)

artist_id_stats = (
    enriched_artist
    .groupBy("artist_id")
    .agg(F.avg("release_date_quarter").alias("avg_release_date_quarter"))
)

compared = (
    enriched_artist.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.release_date_quarter") > F.col("g.avg_release_date_quarter"))
    .select(
        "s.*",
        "g.avg_release_date_quarter",
        F.round(F.col("s.release_date_quarter") - F.col("g.avg_release_date_quarter"), 2).alias("release_date_quarter_minus_avg"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.max("ms_played").alias("max_play_ms_played"))
)

profiled = (
    compared.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.max_play_ms_played")
)

result = (
    profiled
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "is_explicit_label",
        "artist_name",
        "avg_release_date_quarter",
        "release_date_quarter_minus_avg",
        "max_play_ms_played",
        F.when(F.col("is_explicit"), 1).otherwise(0).alias("is_explicit_flag"),
    )
    .orderBy("id")
)
