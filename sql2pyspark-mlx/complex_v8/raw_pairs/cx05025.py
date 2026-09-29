from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc(), "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").desc())
w_is_explicit = Window.partitionBy("is_explicit")

recent = df_tracks.filter(F.col("title").isNotNull())

windowed = (
    recent
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.ntile(3).over(w_is_explicit_by_duration_sec).alias("duration_sec_tercile"),
        F.rank().over(w_is_explicit_by_duration_sec_2).alias("duration_sec_rank"),
        F.max("duration_sec").over(w_is_explicit).alias("is_explicit_max_duration_sec"),
    )
)

with_parent_artist = (
    windowed.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.release_date",
        "s.bpm",
        "s.moods",
        "s.duration_sec_tercile",
        "s.duration_sec_rank",
        "s.is_explicit_max_duration_sec",
        F.col("a.country").alias("artist_country"),
        F.col("a.is_verified").alias("artist_is_verified"),
    )
)

detail_rows = (
    with_parent_artist.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.moods",
        "s.duration_sec_rank",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.source").alias("play_source"),
        F.col("p.ms_played").alias("play_ms_played"),
    )
)

result = (
    detail_rows
    .select(
        "id",
        "play_id",
        "duration_sec_rank",
        "play_ms_played",
    )
    .unpivot(
        ids=["id", "play_id"],
        values=["duration_sec_rank", "play_ms_played"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
