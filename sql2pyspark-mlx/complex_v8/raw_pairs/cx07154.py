from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_duration_sec_ru = Window.partitionBy("artist_id").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_artist_id_by_duration_sec_ti = Window.partitionBy("artist_id").orderBy("duration_sec")
w_has_workout_is_explicit_by_b = Window.partitionBy("has_workout", "is_explicit").orderBy("bpm_distance", "id")
w_has_workout_is_explicit_by_b_2 = Window.partitionBy("has_workout", "is_explicit").orderBy("bpm_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_explicit_has_workout = Window.partitionBy("is_explicit", "has_workout")

scoped_tracks = (
    df_tracks
    .filter(F.col("bpm").between(94, 166))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "is_explicit",
        "bpm",
        F.array_contains(F.col("moods"), "workout").alias("has_workout"),
    )
)

windowed = (
    scoped_tracks
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w_artist_id_by_duration_sec_ru))
    .withColumn("duration_sec_cume_dist", F.round(F.cume_dist().over(w_artist_id_by_duration_sec_ti), 3))
)

derived = (
    windowed
    .filter(~F.col("is_explicit"))
    .select(
        "*",
        F.lower(F.trim(F.col("title"))).alias("title_clean"),
        F.abs(F.col("bpm") - 147).alias("bpm_distance"),
    )
)

with_trends = (
    derived
    .select(
        "*",
        F.round(F.cume_dist().over(w_has_workout_is_explicit_by_b), 3).alias("bpm_distance_cume_dist"),
        F.last("bpm_distance").over(w_has_workout_is_explicit_by_b_2).alias("last_bpm_distance"),
        F.round(F.col("bpm_distance") - F.avg("bpm_distance").over(w_is_explicit_has_workout), 2).alias("bpm_distance_vs_is_explicit_has_workout_avg"),
        (F.col("bpm_distance") - F.lag("bpm_distance").over(w_has_workout_is_explicit_by_b)).alias("bpm_distance_change"),
    )
)

result = (
    with_trends
    .cube("has_workout", "artist_id")
    .agg(
        F.round(F.sum("bpm_distance_vs_is_explicit_has_workout_avg") / F.nullif(F.sum("bpm_distance_change"), F.lit(0)), 3).alias("bpm_distance_vs_is_explicit_has_workout_avg_to_b"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("has_workout").asc_nulls_last(),
        F.col("artist_id").asc_nulls_last(),
    )
)
