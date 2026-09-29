from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_explicit_artist_id_by_dur = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec")
w_is_explicit_artist_id_by_dur_2 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_is_explicit = Window.partitionBy("artist_id", "is_explicit")
w_is_explicit_artist_id_by_dur_3 = Window.partitionBy("is_explicit", "artist_id").orderBy("duration_sec", "id").rowsBetween(-1, -1)
w_duration_sec_rank_first_play = Window.partitionBy("duration_sec_rank", F.date_trunc("month", "first_played_at")).orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_played_at_runnin = Window.orderBy("last_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_duration_sec_rank_last_playe = Window.partitionBy("duration_sec_rank", F.date_trunc("month", "last_played_at"))
w_duration_sec_rank_first_play_2 = Window.partitionBy("duration_sec_rank", F.date_trunc("month", "first_played_at")).orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_last_played_at = Window.orderBy("last_played_at", "id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy(F.col("duration_sec").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_last_played_at_full = Window.orderBy("last_played_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_tracks = (
    df_tracks
    .filter((~F.col("is_explicit")) & (F.array_contains(F.col("moods"), "party")))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.coalesce(F.col("duration_sec"), F.lit(0)).alias("duration_sec_filled"),
        F.round(F.col("duration_sec") / F.nullif(F.col("bpm"), F.lit(0)), 3).alias("duration_sec_per_bpm"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
    )
)

with_windows = (
    scoped_tracks
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "duration_sec_filled",
        "days_since_release_date",
        F.rank().over(w_is_explicit_artist_id_by_dur).alias("duration_sec_rank"),
        F.first("bpm", ignorenulls=True).over(w_is_explicit_artist_id_by_dur_2).alias("first_known_bpm"),
        F.round(F.percent_rank().over(w_is_explicit_artist_id_by_dur), 3).alias("duration_sec_percent_rank"),
        F.round(F.col("duration_sec_filled") / F.nullif(F.sum("duration_sec_filled").over(w_artist_id_is_explicit), F.lit(0)), 4).alias("duration_sec_filled_share_of_artist_id_is_explic"),
        F.round(F.avg("days_since_release_date").over(w_is_explicit_artist_id_by_dur_3), 2).alias("days_since_release_date_avg_1_prior"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.min("played_at").alias("first_played_at"),
        F.max("played_at").alias("last_played_at"),
    )
)

profiled = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", "p.first_played_at", "p.last_played_at")
)

top_rows = profiled.filter(F.col("duration_sec_rank") == 1)

windowed = (
    top_rows
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "duration_sec_rank",
        "first_known_bpm",
        "duration_sec_percent_rank",
        F.last("first_known_bpm").over(w_duration_sec_rank_first_play).alias("last_first_known_bpm"),
        F.sum("first_known_bpm").over(w_all_by_last_played_at_runnin).alias("running_first_known_bpm"),
        F.max("days_since_release_date").over(w_duration_sec_rank_first_play_2).alias("max_days_since_release_date_so_far"),
        F.lag("last_played_at").over(w_all_by_last_played_at).alias("prev_last_played_at"),
        F.last("first_known_bpm").over(w_is_explicit_by_duration_sec).alias("last_first_known_bpm_2"),
        F.nth_value("duration_sec", 2).over(w_all_by_last_played_at_full).alias("second_duration_sec"),
    )
)

play_stats_2 = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.max("played_at").alias("last_played_at"),
        F.count("*").alias("play_count"),
    )
)

summarized_rows = (
    windowed.alias("s")
    .join(play_stats_2.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "p.last_played_at",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
    )
)

result = (
    summarized_rows
    .select(
        "id",
        "duration_sec",
        "is_explicit",
        "bpm",
        "days_since_release_date",
        "duration_sec_rank",
        "first_known_bpm",
        "duration_sec_percent_rank",
        "last_first_known_bpm",
        "running_first_known_bpm",
        "second_duration_sec",
        "last_played_at",
        "play_count",
        F.abs(F.col("last_first_known_bpm_2") - 108).alias("last_first_known_bpm_2_distance"),
    )
)
