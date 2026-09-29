from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_track_id_by_ms_played_distan = Window.partitionBy("track_id").orderBy("ms_played_distance", "id").rowsBetween(-1, Window.currentRow)
w_device_group_played_at_to_da = Window.partitionBy("device_group", F.to_date("played_at")).orderBy(F.col("ms_played_distance").asc_nulls_last())
w_track_id_by_ms_played_distan_2 = Window.partitionBy("track_id").orderBy("ms_played_distance", "id")
w_device_group_played_at_to_da_2 = Window.partitionBy("device_group", F.to_date("played_at")).orderBy(F.col("ms_played_distance").asc_nulls_last(), "id")
w_device_group = Window.partitionBy("device_group")
w_is_skipped_label_ms_played_d = Window.partitionBy("is_skipped_label", "ms_played_distance_quintile").orderBy("ms_played_distance_decile", "id")
w_all_by_ms_played = Window.orderBy(F.col("ms_played").desc(), "id")

valid = (
    df_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        F.abs(F.col("ms_played") - 178909).alias("ms_played_distance"),
        F.when(F.col("device").isin("car", "speaker"), "group_a").when(F.col("device") == "tv", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

sequenced = (
    valid
    .select(
        "*",
        F.sum("ms_played_distance").over(w_track_id).alias("track_id_sum_ms_played_distance"),
        F.sum("ms_played_distance").over(w_track_id_by_ms_played_distan).alias("ms_played_distance_sum_2"),
        F.round(F.cume_dist().over(w_device_group_played_at_to_da), 3).alias("ms_played_distance_cume_dist"),
        F.ntile(10).over(w_track_id_by_ms_played_distan_2).alias("ms_played_distance_decile"),
        F.count(F.lit(1)).over(w_track_id).alias("rows_in_track_id"),
        F.ntile(5).over(w_device_group_played_at_to_da_2).alias("ms_played_distance_quintile"),
    )
)

with_activity = (
    sequenced.alias("s")
    .join(df_tracks.alias("t"), (F.col("t.id") == F.col("s.track_id")) & (F.upper(F.trim(F.col("t.title"))).like("R%")), "left_anti")
)

play_stats = (
    df_plays
    .groupBy("track_id")
    .agg(F.count("*").alias("play_count"))
)

profiled = (
    with_activity.alias("s")
    .join(play_stats.alias("p"), F.col("p.track_id") == F.col("s.track_id"), "left")
    .select("s.*", F.coalesce(F.col("p.play_count"), F.lit(0)).alias("play_count"))
)

with_trends = (
    profiled
    .select(
        "*",
        F.round(F.col("ms_played_distance_decile") / F.nullif(F.sum("ms_played_distance_decile").over(w_device_group), F.lit(0)), 4).alias("ms_played_distance_decile_share_of_device_group"),
        F.lag("ms_played_distance_sum_2").over(w_is_skipped_label_ms_played_d).alias("prev_ms_played_distance_sum_2"),
        F.count(F.lit(1)).over(w_device_group).alias("rows_in_device_group"),
        F.size(F.collect_set("listener_id").over(w_device_group)).alias("distinct_listener_id_in_device_group"),
        F.ntile(5).over(w_all_by_ms_played).alias("ms_played_quintile"),
    )
)

with_activity_2 = (
    with_trends.alias("s")
    .join(df_plays.alias("p"), (F.col("p.track_id") == F.col("s.track_id")) & (F.col("p.listener_id") >= 5), "left_semi")
)

result = (
    with_activity_2
    .filter(F.col("track_id_sum_ms_played_distance").between(600000, 3000000))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "ms_played_distance",
        "is_skipped_label",
        "track_id_sum_ms_played_distance",
        "ms_played_distance_sum_2",
        "ms_played_distance_cume_dist",
        "ms_played_distance_decile",
        "rows_in_track_id",
        "ms_played_distance_quintile",
        "play_count",
        "ms_played_distance_decile_share_of_device_group",
        "prev_ms_played_distance_sum_2",
        "rows_in_device_group",
        "distinct_listener_id_in_device_group",
        (F.floor(F.col("ms_played_distance") / 50000) * 50000).alias("ms_played_distance_bucket"),
        F.lower(F.regexp_replace(F.trim(F.col("is_skipped_label")), " ", "_")).alias("is_skipped_label_slug"),
    )
    .orderBy(
        F.col("is_skipped_label_slug").asc(),
        F.col("ms_played_distance_decile").asc(),
    )
)
