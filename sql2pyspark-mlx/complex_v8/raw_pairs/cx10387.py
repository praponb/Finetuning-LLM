from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_artist_id_by_total_bpm_ties = Window.partitionBy("artist_id").orderBy(F.col("total_bpm").desc_nulls_first())
w_artist_id_by_total_bpm_runni = Window.partitionBy("artist_id").orderBy(F.col("total_bpm").desc_nulls_first(), "play_device", "artist_id", "play_played_at_2_month").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_play_device_artist_id_by_tot = Window.partitionBy("play_device", "artist_id").orderBy(F.col("total_bpm").desc())
w_artist_id_by_total_bpm_prior = Window.partitionBy("artist_id").orderBy("total_bpm", "play_device", "artist_id", "play_played_at_2_month").rowsBetween(-2, -1)
w_artist_id_play_device = Window.partitionBy("artist_id", "play_device")
w_artist_id_by_total_bpm_ties_2 = Window.partitionBy("artist_id").orderBy("total_bpm")
w_total_bpm_dense_rank_artist = Window.partitionBy("total_bpm_dense_rank", "artist_id").orderBy("total_bpm_dense_rank", "play_device", "artist_id", "play_played_at_2_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_device = Window.partitionBy("play_device")
w_play_device_by_last_known_to = Window.partitionBy("play_device").orderBy("last_known_total_bpm")
w_play_device_by_last_known_to_2 = Window.partitionBy("play_device").orderBy("last_known_total_bpm", "play_device", "artist_id", "play_played_at_2_month").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_play_device_by_last_known_to_3 = Window.partitionBy("play_device").orderBy("last_known_total_bpm", "play_device", "artist_id", "play_played_at_2_month")

valid_tracks = (
    df_tracks
    .filter(F.col("bpm") >= 75)
    .select(
        "id",
        "artist_id",
        "title",
        "duration_sec",
        "release_date",
        "is_explicit",
        "bpm",
        F.abs(F.col("duration_sec") - 279).alias("duration_sec_distance"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("release_date")).alias("days_since_release_date"),
        F.substring(F.trim(F.col("title")), 1, 2).alias("title_prefix"),
    )
)

detail_rows_plays = (
    valid_tracks.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.artist_id",
        "s.duration_sec",
        "s.release_date",
        "s.is_explicit",
        "s.bpm",
        F.col("p.id").alias("play_id"),
        F.col("p.played_at").alias("play_played_at"),
        F.col("p.listener_id").alias("play_listener_id"),
    )
)

joined_listener = (
    detail_rows_plays.alias("s")
    .join(df_listeners.alias("l"), F.col("l.id") == F.col("s.play_listener_id"), "left")
    .join(df_plays.alias("p"), F.col("p.id") == F.col("s.play_id"), "left")
    .select(
        "s.id",
        "s.artist_id",
        "s.is_explicit",
        "s.bpm",
        "s.play_id",
        "s.play_listener_id",
        F.col("l.country").alias("listener_country"),
        F.col("l.username").alias("listener_username"),
        F.col("l.plan").alias("listener_plan"),
        F.col("p.played_at").alias("play_played_at_2"),
        F.col("p.device").alias("play_device"),
    )
)

per_group = (
    joined_listener
    .groupBy(
        "play_device",
        "artist_id",
        F.date_trunc("month", "play_played_at_2").alias("play_played_at_2_month"),
    )
    .agg(
        F.array_sort(F.collect_set("listener_country")).alias("listener_country_list"),
        F.sum("bpm").alias("total_bpm"),
        F.avg("bpm").alias("avg_bpm_check"),
    )
    .filter(F.col("avg_bpm_check") > 89)
    .drop("avg_bpm_check")
)

metrics = (
    per_group
    .select(
        "*",
        F.round(F.cume_dist().over(w_artist_id_by_total_bpm_ties), 3).alias("total_bpm_cume_dist"),
        F.last("total_bpm", ignorenulls=True).over(w_artist_id_by_total_bpm_runni).alias("last_known_total_bpm"),
        F.dense_rank().over(w_play_device_artist_id_by_tot).alias("total_bpm_dense_rank"),
        F.round(F.avg("total_bpm").over(w_artist_id_by_total_bpm_prior), 2).alias("total_bpm_avg_2_prior"),
        F.sum("total_bpm").over(w_artist_id_play_device).alias("artist_id_play_device_sum_total_bpm"),
        F.rank().over(w_artist_id_by_total_bpm_ties_2).alias("total_bpm_rank"),
    )
)

combined_details = (
    metrics.alias("s")
    .join(df_artists.alias("a"), F.col("a.id") == F.col("s.artist_id"), "inner")
    .select(
        "s.play_device",
        "s.artist_id",
        "s.play_played_at_2_month",
        "s.listener_country_list",
        "s.total_bpm_cume_dist",
        "s.last_known_total_bpm",
        "s.total_bpm_dense_rank",
        "s.total_bpm_avg_2_prior",
        "s.total_bpm_rank",
        F.col("a.country").alias("artist_country"),
    )
)

metrics_2 = (
    combined_details
    .select(
        "play_device",
        "artist_id",
        "play_played_at_2_month",
        "listener_country_list",
        "total_bpm_cume_dist",
        "last_known_total_bpm",
        "total_bpm_dense_rank",
        "total_bpm_avg_2_prior",
        "artist_country",
        F.last("total_bpm_avg_2_prior").over(w_total_bpm_dense_rank_artist).alias("last_total_bpm_avg_2_prior"),
        F.round(F.col("total_bpm_avg_2_prior") / F.nullif(F.sum("total_bpm_avg_2_prior").over(w_play_device), F.lit(0)), 4).alias("total_bpm_avg_2_prior_share_of_play_device"),
        F.count(F.lit(1)).over(w_play_device).alias("rows_in_play_device"),
        F.rank().over(w_play_device_by_last_known_to).alias("last_known_total_bpm_rank"),
        F.first("total_bpm_rank").over(w_play_device_by_last_known_to_2).alias("first_total_bpm_rank"),
        F.lag("total_bpm_rank").over(w_play_device_by_last_known_to_3).alias("prev_total_bpm_rank"),
    )
)

result = (
    metrics_2
    .filter(F.col("total_bpm_avg_2_prior").between(315.0, 825.0))
    .select(
        "play_device",
        "artist_id",
        "play_played_at_2_month",
        "listener_country_list",
        "total_bpm_cume_dist",
        "total_bpm_dense_rank",
        "total_bpm_avg_2_prior",
        "artist_country",
        "total_bpm_avg_2_prior_share_of_play_device",
        "rows_in_play_device",
        "first_total_bpm_rank",
        "prev_total_bpm_rank",
        F.round(F.col("total_bpm_cume_dist") * 0.9, 2).alias("total_bpm_cume_dist_adj"),
        F.substring(F.trim(F.col("play_device")), 1, 4).alias("play_device_prefix"),
    )
    .orderBy(F.col("total_bpm_cume_dist").asc_nulls_last())
)
