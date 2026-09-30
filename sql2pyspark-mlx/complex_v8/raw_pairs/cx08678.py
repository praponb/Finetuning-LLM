from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_release_date_month_by_play_p = Window.partitionBy(F.month("release_date")).orderBy(F.col("play_played_at").asc_nulls_last(), "id", "play_id")
w_all_by_duration_sec = Window.orderBy("duration_sec", "id", "play_id")

base = (
    df_tracks
    .filter(F.col("bpm") >= 85)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.date_format(F.col("release_date"), "yyyy-MM-dd").alias("release_date_label"),
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.count(F.when(~F.col("is_explicit"), 1)).alias("track_is_explicit_count"),
    )
)

with_stats = (
    base.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.track_is_explicit_count"), F.lit(0)).alias("track_is_explicit_count"),
    )
)

detail_rows_plays = (
    with_stats.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.moods",
        "s.track_is_explicit_count",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.source").alias("play_source"),
    )
)

windowed = (
    detail_rows_plays
    .withColumn("next2_track_is_explicit_count", F.lead("track_is_explicit_count", 2).over(w_release_date_month_by_play_p))
    .withColumn("duration_sec_cume_dist", F.round(F.cume_dist().over(w_all_by_duration_sec), 3))
)

result = (
    windowed
    .groupingSets(
        [["next2_track_is_explicit_count", "play_source"], ["next2_track_is_explicit_count"], ["play_source"], []],
        "next2_track_is_explicit_count", "play_source",
    )
    .agg(
        F.max("track_is_explicit_count").alias("max_track_is_explicit_count"),
        F.count("*").alias("row_count"),
    )
)
