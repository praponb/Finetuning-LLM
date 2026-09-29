from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_play_played_at = Window.partitionBy("artist_id").orderBy("play_played_at", "id", "play_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_artist_id_by_play_played_at_2 = Window.partitionBy("artist_id").orderBy("play_played_at")
w_artist_id_play_device = Window.partitionBy("artist_id", "play_device")
w_distinct_play_id_in_artist_i = Window.partitionBy("distinct_play_id_in_artist_id_play_device")
w_play_id_by_bpm = Window.partitionBy("play_id").orderBy("bpm", "id", "play_id")

prepared = (
    df_tracks
    .filter((F.upper(F.trim(F.col("title"))).like("P%")) & (F.col("release_date").between(F.lit("2024-08-26").cast("date"), F.lit("2025-08-28").cast("date"))))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.date_format(F.col("release_date"), "yyyy-MM").alias("release_date_label"),
        (F.col("bpm") - F.col("duration_sec")).alias("bpm_minus_duration_sec"),
        F.least(F.col("bpm"), F.col("duration_sec")).alias("least_bpm_duration_sec"),
    )
)

detail_rows_plays = (
    prepared.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.id")) & (F.col("p.device").isin("tv", "desktop")), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.title",
        "s.duration_sec",
        "s.bpm",
        "s.release_date_label",
        F.col("p.id").alias("play_id"),
        F.col("p.ms_played").alias("play_ms_played"),
        F.col("p.device").alias("play_device"),
        F.col("p.played_at").alias("play_played_at"),
    )
)

sequenced = (
    detail_rows_plays
    .select(
        "*",
        F.nth_value("play_ms_played", 2).over(w_artist_id_by_play_played_at).alias("second_play_ms_played"),
        F.round(F.percent_rank().over(w_artist_id_by_play_played_at_2), 3).alias("play_played_at_percent_rank"),
        F.size(F.collect_set("play_id").over(w_artist_id_play_device)).alias("distinct_play_id_in_artist_id_play_device"),
    )
)

windowed = (
    sequenced
    .select(
        "id",
        "artist_id",
        "title",
        "bpm",
        "release_date_label",
        "play_id",
        "play_ms_played",
        "play_device",
        "second_play_ms_played",
        "play_played_at_percent_rank",
        "distinct_play_id_in_artist_id_play_device",
        F.sum("play_ms_played").over(w_distinct_play_id_in_artist_i).alias("distinct_play_id_in_artist_id_play_device_sum_pl"),
        F.rank().over(w_play_id_by_bpm).alias("bpm_rank"),
        F.count(F.lit(1)).over(w_distinct_play_id_in_artist_i).alias("rows_in_distinct_play_id_in_artist_id_play_devic"),
        F.min("duration_sec").over(w_distinct_play_id_in_artist_i).alias("distinct_play_id_in_artist_id_play_device_min_du"),
    )
)

with_activity = (
    windowed.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.bpm") < 127) & (F.size(F.col("t.moods")) >= 1), "left_semi")
)

result = (
    with_activity
    .groupBy("distinct_play_id_in_artist_id_play_device")
    .pivot("play_device", ["speaker", "desktop", "phone"])
    .agg(F.count("second_play_ms_played"))
)
