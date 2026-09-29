from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_len_device = Window.partitionBy("device_len", "device")
w_all_by_device_len_ties = Window.orderBy(F.col("device_len").desc())
w_all_by_device_len_full = Window.orderBy(F.col("device_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_device_len_dense_ra = Window.partitionBy("track_id", "device_len_dense_rank")
w_track_id_device_len_dense_ra_2 = Window.partitionBy("track_id", "device_len_dense_rank").orderBy("track_duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_track_id_device_len_dense_ra_3 = Window.partitionBy("track_id", "device_len_dense_rank").orderBy("track_duration_sec", "id").rowsBetween(-1, -1)

recent_plays = (
    df_plays
    .filter((F.col("source") == "album") & (F.col("listener_id") <= 4))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "device",
        "source",
        F.concat_ws(" / ", F.col("device"), F.col("source")).alias("device_source_label"),
        F.upper(F.trim(F.col("device"))).alias("device_clean"),
        F.length(F.trim(F.col("device"))).alias("device_len"),
    )
)

filtered = (
    recent_plays.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (~F.col("t.is_explicit")), "left_semi")
)

source_stats = (
    filtered
    .groupBy("source")
    .agg(F.avg("device_len").alias("avg_device_len"))
)

compared = (
    filtered.alias("s")
    .join(source_stats.alias("g"), F.col("g.source") == F.col("s.source"), "inner")
    .filter(F.col("s.device_len") > F.col("g.avg_device_len"))
    .select(
        "s.*",
        "g.avg_device_len",
        F.round(F.col("s.device_len") - F.col("g.avg_device_len"), 2).alias("device_len_minus_avg"),
    )
)

with_activity = (
    compared.alias("s")
    .join(df_plays.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left_semi")
)

with_trends = (
    with_activity
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "device_source_label",
        "device_clean",
        "avg_device_len",
        "device_len_minus_avg",
        F.round(F.col("avg_device_len") - F.avg("avg_device_len").over(w_device_len_device), 2).alias("avg_device_len_vs_device_len_device_avg"),
        F.dense_rank().over(w_all_by_device_len_ties).alias("device_len_dense_rank"),
        F.nth_value("avg_device_len", 2).over(w_all_by_device_len_full).alias("second_avg_device_len"),
    )
)

joined = (
    with_trends.alias("s")
    .join(df_tracks.alias("t"), F.col("t.id") == F.col("s.track_id"), "left")
    .select(
        "s.id",
        "s.track_id",
        "s.played_at",
        "s.source",
        "s.device_source_label",
        "s.device_clean",
        "s.avg_device_len",
        "s.device_len_minus_avg",
        "s.device_len_dense_rank",
        "s.second_avg_device_len",
        F.col("t.release_date").alias("track_release_date"),
        F.col("t.moods").alias("track_moods"),
        F.col("t.duration_sec").alias("track_duration_sec"),
    )
)

metrics = (
    joined
    .select(
        "*",
        F.round(F.avg("avg_device_len").over(w_track_id_device_len_dense_ra), 2).alias("track_id_device_len_dense_rank_avg_avg_device_le"),
        F.size(F.collect_set("source").over(w_track_id_device_len_dense_ra)).alias("distinct_source_in_track_id_device_len_dense_ran"),
        F.sum("second_avg_device_len").over(w_track_id_device_len_dense_ra).alias("track_id_device_len_dense_rank_sum_second_avg_de"),
        F.last("track_release_date").over(w_track_id_device_len_dense_ra_2).alias("last_track_release_date"),
        F.round(F.col("device_len_minus_avg") - F.avg("device_len_minus_avg").over(w_track_id_device_len_dense_ra), 2).alias("device_len_minus_avg_vs_track_id_device_len_dens"),
        F.sum("avg_device_len").over(w_track_id_device_len_dense_ra_3).alias("avg_device_len_sum_1_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "track_id",
        "played_at",
        "source",
        "device_source_label",
        "device_clean",
        "device_len_minus_avg",
        "second_avg_device_len",
        "track_release_date",
        "track_id_device_len_dense_rank_sum_second_avg_de",
        "last_track_release_date",
        "device_len_minus_avg_vs_track_id_device_len_dens",
        F.datediff(F.col("track_release_date"), F.col("last_track_release_date")).alias("days_last_track_release_date_to_track_release_da"),
        F.coalesce(F.col("avg_device_len"), F.lit(0.0)).alias("avg_device_len_filled"),
    )
    .orderBy(F.col("played_at").desc(), "id")
    .limit(5)
)
