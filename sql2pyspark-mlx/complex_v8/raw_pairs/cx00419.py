from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id")

recent_tracks = (
    df_tracks
    .filter((F.col("title").isNotNull()) & (F.col("bpm") > 94))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
    )
)

metrics = (
    recent_tracks
    .withColumn("duration_sec_share_of_artist_id", F.round(F.col("duration_sec") / F.nullif(F.sum("duration_sec").over(w_artist_id), F.lit(0)), 4))
    .withColumn("duration_sec_seq", F.row_number().over(w_is_explicit_artist_id_by_dur))
)

line_level = (
    metrics.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.played_at") < F.lit("2025-03-02 11:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.duration_sec_share_of_artist_id",
        "s.duration_sec_seq",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    line_level
    .filter(F.col("play_ms_played").between(67932, 359437))
    .select("duration_sec_seq")
    .distinct()
    .orderBy(F.col("duration_sec_seq").desc())
)
