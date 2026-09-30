from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_ids_by_cnt_moving_2 = Window.partitionBy("distinct_ids").orderBy(F.col("cnt").desc(), "artist_id").rowsBetween(-2, Window.currentRow)
w_all_by_stddev_bpm_ties = Window.orderBy("stddev_bpm")
w_distinct_ids_by_cnt = Window.partitionBy("distinct_ids").orderBy(F.col("cnt").desc(), "artist_id")
w_distinct_ids_by_cnt_moving_1 = Window.partitionBy("distinct_ids").orderBy(F.col("cnt").desc(), "artist_id").rowsBetween(-1, Window.currentRow)
w_distinct_ids_cnt = Window.partitionBy("distinct_ids", "cnt")

recent_tracks = (
    df_tracks
    .filter((F.col("artist_id") >= 10) & (F.col("duration_sec") <= 352))
    .select(
        "id",
        "artist_id",
        "title",
        "release_date",
        "is_explicit",
        "bpm",
        "moods",
        (F.col("bpm") + F.col("duration_sec")).alias("bpm_plus_duration_sec"),
        F.array_contains(F.col("moods"), "chill").alias("has_chill"),
    )
)

totals = (
    recent_tracks
    .filter(F.col("release_date") < F.lit("2025-08-02").cast("date"))
    .groupBy("artist_id")
    .agg(
        F.round(F.stddev("bpm"), 2).alias("stddev_bpm"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sum(F.when(~F.col("is_explicit"), F.col("bpm_plus_duration_sec")).otherwise(0)).alias("bpm_plus_duration_sec_not_is_explicit"),
        F.round(F.avg("bpm_plus_duration_sec"), 2).alias("avg_bpm_plus_duration_sec"),
    )
)

ranked = (
    totals
    .select(
        "*",
        F.sum("distinct_ids").over(w_distinct_ids_by_cnt_moving_2).alias("distinct_ids_sum_3"),
        F.dense_rank().over(w_all_by_stddev_bpm_ties).alias("stddev_bpm_dense_rank"),
        F.lead("avg_bpm_plus_duration_sec", 3, 0).over(w_distinct_ids_by_cnt).alias("next3_avg_bpm_plus_duration_sec"),
        F.row_number().over(w_distinct_ids_by_cnt).alias("rn"),
        F.max("distinct_ids").over(w_distinct_ids_by_cnt_moving_1).alias("distinct_ids_max_2"),
        F.max("stddev_bpm").over(w_distinct_ids_cnt).alias("distinct_ids_cnt_max_stddev_bpm"),
    )
)

with_children = (
    ranked.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.release_date").isNotNull()), "inner")
    .select(
        "s.artist_id",
        "s.bpm_plus_duration_sec_not_is_explicit",
        "s.stddev_bpm_dense_rank",
        "s.next3_avg_bpm_plus_duration_sec",
        "s.rn",
        F.col("t.id").alias("track_id"),
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

result = (
    with_children
    .select(
        "artist_id",
        "bpm_plus_duration_sec_not_is_explicit",
        "stddev_bpm_dense_rank",
        "next3_avg_bpm_plus_duration_sec",
        "rn",
        "track_id",
        "track_release_date",
        "track_duration_sec",
        F.abs(F.col("track_duration_sec") - 271).alias("track_duration_sec_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("track_release_date")).alias("days_since_track_release_date"),
    )
    .orderBy("bpm_plus_duration_sec_not_is_explicit", F.col("rn").desc())
)
