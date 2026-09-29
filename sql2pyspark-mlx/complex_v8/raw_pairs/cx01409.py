from pyspark.sql import functions as F

scoped_tracks = (
    df_tracks
    .filter(F.col("is_explicit"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "bpm",
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.abs(F.col("duration_sec") - 209).alias("duration_sec_distance"),
        F.year(F.col("release_date")).alias("release_date_year"),
    )
)

joined = (
    scoped_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        "s.greatest_bpm_duration_sec",
        "s.release_date_year",
        F.col("a.country").alias("artist_country"),
    )
)

exploded_detail = (
    joined.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    exploded_detail
    .groupBy("artist_id")
    .pivot("play_device", ["tv", "phone", "speaker"])
    .agg(F.sum("duration_sec"))
)
