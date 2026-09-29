from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_bpm_moving_1 = Window.partitionBy("artist_id").orderBy(F.col("bpm").desc(), "id").rowsBetween(-1, Window.currentRow)
w_artist_id = Window.partitionBy("artist_id")

valid_tracks = (
    df_tracks
    .filter(F.array_contains(F.col("moods"), "party"))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        F.concat(F.col("title"), F.lit("#"), F.col("duration_sec").cast("string")).alias("title_ref"),
    )
)

combined_details = (
    valid_tracks.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        "s.moods",
        "a.artist_name",
    )
)

sequenced = (
    combined_details
    .select(
        "id",
        "duration_sec",
        "release_date",
        "is_explicit",
        "moods",
        "artist_name",
        F.sum("duration_sec").over(w_artist_id_by_bpm_moving_1).alias("duration_sec_sum_2"),
        F.round(F.col("duration_sec") - F.avg("duration_sec").over(w_artist_id), 2).alias("duration_sec_vs_artist_id_avg"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count(F.when(F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
        F.count("*").alias("play_count"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled = (
    sequenced.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.last_played_at",
    )
)

result = (
    profiled
    .rollup("play_is_skipped_count", "play_count", "is_explicit")
    .agg(
        F.array_sort(F.collect_set("artist_name")).alias("artist_name_list"),
        F.percentile_approx("play_count", 0.5).alias("median_play_count"),
        F.min("last_played_at").alias("first_last_played_at"),
    )
)
