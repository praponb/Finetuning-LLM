from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_by_bpm = Window.partitionBy("play_count").orderBy("bpm", "id")
w_is_explicit = Window.partitionBy("is_explicit")
w_play_is_skipped_count_releas = Window.partitionBy("play_is_skipped_count", F.date_trunc("month", "release_date")).orderBy("seq_no", "id")
w_seq_no = Window.partitionBy("seq_no")
w_play_is_skipped_count_releas_2 = Window.partitionBy("play_is_skipped_count", F.date_trunc("month", "release_date")).orderBy("seq_no")

cleaned = (
    df_tracks
    .filter((F.length(F.trim(F.col("title"))) >= 8) & (F.col("duration_sec") >= 209))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.least(F.col("duration_sec"), F.col("bpm")).alias("least_duration_sec_bpm"),
        F.add_months(F.col("release_date"), 3).alias("release_date_plus_3m"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("ms_played").alias("max_play_ms_played"),
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

with_counts = (
    cleaned.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.max_play_ms_played",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

with_windows = (
    with_counts
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "last_played_at",
        (F.col("bpm") - F.lag("bpm").over(w_play_count_by_bpm)).alias("bpm_change"),
        F.row_number().over(w_play_count_by_bpm).alias("seq_no"),
        F.count(F.lit(1)).over(w_is_explicit).alias("rows_in_is_explicit"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.count(F.when(~F.col("is_skipped"), 1)).alias("play_is_skipped_count"),
    )
)

with_stats = (
    with_windows.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.first_played_at",
        F.coalesce(F.col("p.play_is_skipped_count"), F.lit(0)).alias("play_is_skipped_count"),
    )
)

with_windows_2 = (
    with_stats
    .select(
        "*",
        (F.col("bpm") - F.lag("bpm").over(w_play_is_skipped_count_releas)).alias("bpm_change_2"),
        F.count(F.lit(1)).over(w_seq_no).alias("rows_in_seq_no"),
        F.dense_rank().over(w_play_is_skipped_count_releas_2).alias("seq_no_dense_rank"),
    )
)

result = (
    with_windows_2
    .filter(F.to_date(F.col("last_played_at")) < F.lit("2025-02-05").cast("date"))
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "last_played_at",
        "bpm_change",
        "first_played_at",
        "play_is_skipped_count",
        "bpm_change_2",
        "rows_in_seq_no",
        "seq_no_dense_rank",
    )
    .orderBy("bpm_change_2", "id")
    .limit(20)
)
