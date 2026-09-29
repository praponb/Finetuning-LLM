from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id = Window.partitionBy("artist_id")
w_release_date_month_artist_id = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_release_date_month_artist_id_2 = Window.partitionBy(F.month("release_date"), "artist_id").orderBy(F.col("bpm_distance").desc(), "id")
w_all_by_bpm_distance = Window.orderBy(F.col("bpm_distance").desc(), "id")
w_all_by_bpm_distance_prior_3 = Window.orderBy(F.col("bpm_distance").desc(), "id").rowsBetween(-3, -1)

scoped = (
    df_tracks
    .filter(F.upper(F.trim(F.col("title"))).like("R%"))
    .select(
        "id",
        "artist_id",
        "duration_sec",
        "release_date",
        "moods",
        F.abs(F.col("bpm") - 104).alias("bpm_distance"),
        F.date_trunc("quarter", F.col("release_date")).alias("release_date_quarter"),
    )
)

metrics = (
    scoped
    .select(
        "id",
        "duration_sec",
        "release_date",
        "bpm_distance",
        F.nth_value("release_date", 2).over(w_release_date_month_artist_id).alias("second_release_date"),
        (F.col("duration_sec") - F.lag("duration_sec").over(w_release_date_month_artist_id_2)).alias("duration_sec_change"),
        F.round(F.col("bpm_distance") / F.nullif(F.sum("bpm_distance").over(w_artist_id), F.lit(0)), 4).alias("bpm_distance_share_of_artist_id"),
    )
)

with_windows = (
    metrics
    .withColumn("bpm_distance_quartile", F.ntile(4).over(w_all_by_bpm_distance))
    .withColumn("duration_sec_change_avg_3_prior", F.round(F.avg("duration_sec_change").over(w_all_by_bpm_distance_prior_3), 2))
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

with_stats = (
    with_windows.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

result = (
    with_stats
    .groupingSets(
        [["play_count", "bpm_distance_quartile"], ["play_count"], []],
        "play_count", "bpm_distance_quartile",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.datediff(F.max("release_date"), F.min("release_date")).alias("release_date_span_days"),
        F.round(F.sum("duration_sec_change_avg_3_prior") / F.nullif(F.sum("duration_sec"), F.lit(0)), 3).alias("duration_sec_change_avg_3_prior_to_duration_sec_"),
        F.round(F.sum("duration_sec_change_avg_3_prior") / F.nullif(F.sum("bpm_distance_share_of_artist_id"), F.lit(0)), 3).alias("duration_sec_change_avg_3_prior_to_bpm_distance_"),
    )
)
