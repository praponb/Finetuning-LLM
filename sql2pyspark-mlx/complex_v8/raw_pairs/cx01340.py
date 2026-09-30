from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit = Window.partitionBy("is_explicit")

cleaned_tracks = (
    df_tracks
    .filter((F.col("release_date").between(F.lit("2024-04-21").cast("date"), F.lit("2025-04-02").cast("date"))) & (F.col("duration_sec") > 142))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        "moods",
    )
)

track_stats = (
    df_tracks
    .groupBy("artist_id")
    .agg(F.min("duration_sec").alias("min_track_duration_sec"))
)

summarized_rows = (
    cleaned_tracks.alias("s")
    .join(track_stats.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.min_track_duration_sec")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
        F.min("ms_played").alias("min_play_ms_played"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        "p.min_play_ms_played",
    )
)

windowed = summarized_rows_2.withColumn("rows_in_is_explicit", F.count(F.lit(1)).over(w_is_explicit))

artist_id_stats = (
    windowed
    .groupBy("artist_id")
    .agg(F.max("min_play_ms_played").alias("max_min_play_ms_played"))
)

above_average = (
    windowed.alias("s")
    .join(artist_id_stats.alias("g"), F.col("g.artist_id") == F.col("s.artist_id"), "inner")
    .filter(F.col("s.min_play_ms_played") == F.col("g.max_min_play_ms_played"))
    .select(
        "s.*",
        "g.max_min_play_ms_played",
        F.round(F.col("s.min_play_ms_played") - F.col("g.max_min_play_ms_played"), 2).alias("min_play_ms_played_minus_max"),
    )
)

screened = (
    above_average.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") >= 82) & (F.size(F.col("t.moods")) >= 2), "left_anti")
    .filter(F.to_date(F.col("last_played_at")) < F.lit("2025-02-13").cast("date"))
)

track_stats_2 = (
    df_tracks
    .groupBy("artist_id")
    .agg(
        F.min("release_date").alias("first_track_release_date"),
        F.min("bpm").alias("min_track_bpm"),
    )
)

profiled = (
    screened.alias("s")
    .join(track_stats_2.alias("t"), F.col("t.artist_id") == F.col("s.artist_id"), "left")
    .select("s.*", "t.first_track_release_date", "t.min_track_bpm")
)

result = (
    profiled
    .rollup("artist_id", "is_explicit")
    .agg(
        F.grouping_id().alias("gid"),
        F.round(F.sum("max_min_play_ms_played") / F.nullif(F.sum("min_play_ms_played"), F.lit(0)), 3).alias("max_min_play_ms_played_to_min_play_ms_played_rat"),
    )
    .orderBy(
        F.col("artist_id").asc_nulls_last(),
        F.col("is_explicit").asc_nulls_last(),
    )
)
