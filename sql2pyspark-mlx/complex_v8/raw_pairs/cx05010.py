from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_play_count_is_explicit_by_pl = Window.partitionBy("play_count", "is_explicit").orderBy("play_count_2", "id")
w_release_date_year = Window.partitionBy(F.year("release_date"))
w_play_count_is_explicit_by_pl_2 = Window.partitionBy("play_count", "is_explicit").orderBy("play_count_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_count_is_explicit_by_pl_3 = Window.partitionBy("play_count", "is_explicit").orderBy("play_count_2", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = df_tracks.filter((F.array_contains(F.col("moods"), "party")) & (F.col("duration_sec") > 165))

scored = (
    scoped
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        F.abs(F.col("duration_sec") - 209).alias("duration_sec_distance"),
        F.greatest(F.col("bpm"), F.col("duration_sec")).alias("greatest_bpm_duration_sec"),
        F.abs(F.col("duration_sec") - 352).alias("duration_sec_distance_2"),
    )
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(
        F.count("*").alias("play_count"),
        F.count("*").alias("play_count_2"),
        F.min("played_at").alias("first_played_at"),
    )
)

with_stats = (
    scored.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"),
        F.coalesce(F.col("p.play_count_2"), F.lit(0)).alias("play_count_2"),
        "p.first_played_at",
    )
)

with_windows = (
    with_stats
    .select(
        "id",
        "title",
        "release_date",
        "is_explicit",
        "duration_sec_distance",
        "greatest_bpm_duration_sec",
        "duration_sec_distance_2",
        "play_count",
        "play_count_2",
        "first_played_at",
        (F.col("duration_sec_distance_2") - F.lag("duration_sec_distance_2").over(w_play_count_is_explicit_by_pl)).alias("duration_sec_distance_2_change"),
        F.round(F.avg("greatest_bpm_duration_sec").over(w_release_date_year), 2).alias("release_date_year_avg_greatest_bpm_duration_sec"),
        F.rank().over(w_play_count_is_explicit_by_pl).alias("play_count_2_rank"),
        F.sum("greatest_bpm_duration_sec").over(w_play_count_is_explicit_by_pl_2).alias("running_greatest_bpm_duration_sec"),
        F.last("first_played_at", ignorenulls=True).over(w_play_count_is_explicit_by_pl_2).alias("last_known_first_played_at"),
        F.first("greatest_bpm_duration_sec").over(w_play_count_is_explicit_by_pl_3).alias("first_greatest_bpm_duration_sec"),
    )
)

result = (
    with_windows
    .groupingSets(
        [["is_explicit", "play_count_2"], ["is_explicit"], []],
        "is_explicit", "play_count_2",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("play_count_2").alias("total_play_count_2"),
    )
    .orderBy(
        F.col("is_explicit").asc_nulls_last(),
        F.col("play_count_2").asc_nulls_last(),
    )
)
