from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_is_explicit_by_bpm = Window.partitionBy("artist_id", "is_explicit").orderBy("bpm", "id")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_has_workout = Window.partitionBy("is_explicit", "has_workout")
w_has_workout_by_bpm = Window.partitionBy("has_workout").orderBy("bpm", "id")
w_all_by_bpm_full = Window.orderBy(F.col("bpm").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_tracks = (
    df_tracks
    .filter((F.col("artist_id").isNotNull()) & (F.year(F.col("release_date")) == 2024))
    .select(
        "id",
        "artist_id",
        "is_explicit",
        "bpm",
        "moods",
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

ranked = (
    scoped_tracks
    .select(
        "*",
        F.lead("bpm").over(w_artist_id_is_explicit_by_bpm).alias("next_bpm"),
        F.round(F.col("bpm") / F.nullif(F.sum("bpm").over(w_artist_id), F.lit(0)), 4).alias("bpm_share_of_artist_id"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_is_explicit_has_workout), 2).alias("bpm_vs_is_explicit_has_workout_avg"),
        F.min("bpm").over(w_artist_id).alias("artist_id_min_bpm"),
        F.rank().over(w_has_workout_by_bpm).alias("bpm_rank"),
        F.last("bpm").over(w_all_by_bpm_full).alias("last_bpm"),
        F.round(F.col("bpm") - F.avg("bpm").over(w_artist_id), 2).alias("bpm_vs_artist_id_avg"),
        F.count(F.lit(1)).over(w_artist_id).alias("rows_in_artist_id"),
    )
)

leaders = ranked.filter(F.col("bpm_rank") == 1)

rollup_base = (
    leaders
    .groupBy("bpm_rank", "artist_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("bpm_vs_is_explicit_has_workout_avg"), 2).alias("avg_bpm_vs_is_explicit_has_workout_avg"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .select(
        "bpm_rank",
        "artist_id",
        "row_count",
        "avg_bpm_vs_is_explicit_has_workout_avg",
        F.abs(F.col("bpm_rank") - 6).alias("bpm_rank_distance"),
    )
    .orderBy(F.col("artist_id").desc_nulls_first(), "bpm_rank")
    .limit(20)
)
