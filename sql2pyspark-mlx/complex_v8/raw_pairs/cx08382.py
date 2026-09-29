from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_release_date = Window.partitionBy("artist_id").orderBy("release_date", "id")
w_artist_id = Window.partitionBy("artist_id")

prepared = (
    df_tracks
    .filter(F.col("artist_id") <= 5)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "moods",
    )
)

ranked = (
    prepared
    .withColumn("release_date_cume_dist", F.round(F.cume_dist().over(w_artist_id_by_release_date), 3))
    .withColumn("duration_sec_vs_artist_id_avg", F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2))
)

line_level_plays = (
    ranked.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.release_date",
        "s.release_date_cume_dist",
        "s.duration_sec_vs_artist_id_avg",
        F.col("p.id").alias("play_id"),
        F.col("p.is_skipped").alias("play_is_skipped"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

result = (
    line_level_plays
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "release_date_cume_dist",
        "play_is_skipped",
        F.dayofweek(F.col("release_date")).alias("release_date_dayofweek"),
        F.substring(F.trim(F.col("title")), 1, 3).alias("title_prefix"),
    )
    .orderBy(F.col("title").desc())
)
