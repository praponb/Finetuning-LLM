from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_duration_sec = Window.orderBy(F.col("duration_sec").desc_nulls_first(), "id")
w_artist_id = Window.partitionBy("artist_id")
w_is_explicit_by_duration_sec = Window.partitionBy("is_explicit").orderBy("duration_sec_percent_rank", "id")
w_is_explicit_by_duration_sec_2 = Window.partitionBy("is_explicit").orderBy("duration_sec_percent_rank", "id").rowsBetween(-1, Window.currentRow)
w_is_explicit_by_duration_sec_3 = Window.partitionBy("is_explicit").orderBy("duration_sec_percent_rank", "id").rowsBetween(-2, Window.currentRow)

recent = (
    df_tracks
    .filter((F.col("release_date") >= F.lit("2024-12-03").cast("date")) | (F.col("bpm") >= 96))
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
    )
)

metrics = (
    recent
    .withColumn("duration_sec_percent_rank", F.round(F.percent_rank().over(w_all_by_duration_sec), 3))
    .withColumn("artist_id_avg_duration_sec", F.round(F.avg("duration_sec").over(w_artist_id), 2))
)

with_trends = (
    metrics
    .select(
        "*",
        F.row_number().over(w_is_explicit_by_duration_sec).alias("rn"),
        F.sum("duration_sec").over(w_is_explicit_by_duration_sec_2).alias("duration_sec_sum_2"),
        F.sum("duration_sec").over(w_is_explicit_by_duration_sec_3).alias("duration_sec_sum_3"),
        F.ntile(3).over(w_is_explicit_by_duration_sec).alias("duration_sec_percent_rank_tercile"),
    )
)

without_matches = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.artist_id") == F.col("s.artist_id")) & (F.col("t.duration_sec").between(144, 352)) & (F.array_contains(F.col("t.moods"), "focus")), "left_semi")
    .filter(F.col("duration_sec_percent_rank_tercile") > 2)
)

result = (
    without_matches
    .rollup("is_explicit", "rn", "duration_sec_percent_rank_tercile")
    .agg(
        F.grouping("is_explicit").alias("is_total_is_explicit"),
        F.round(F.stddev("duration_sec_sum_3"), 2).alias("stddev_duration_sec_sum_3"),
        F.sum("duration_sec_percent_rank_tercile").alias("total_duration_sec_percent_rank_tercile"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
    )
)
