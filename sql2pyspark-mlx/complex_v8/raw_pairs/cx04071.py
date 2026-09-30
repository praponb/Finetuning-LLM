from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id = Window.partitionBy("listener_id")
w_is_skipped_by_ms_played_prio = Window.partitionBy("is_skipped").orderBy("ms_played", "id").rowsBetween(-2, -1)
w_is_skipped_label_by_played_a = Window.partitionBy("is_skipped_label").orderBy(F.col("played_at").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_all_by_ms_played_filled_ties = Window.orderBy("ms_played_filled")
w_track_id_device_group = Window.partitionBy("track_id", "device_group")
w_all_by_played_at_full = Window.orderBy(F.col("played_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_track_id_is_skipped_label = Window.partitionBy("track_id", "is_skipped_label")

filtered_plays = (
    df_plays
    .filter((F.col("ms_played") <= 225733) & (F.col("source").isin("radio", "playlist", "album")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.coalesce(F.col("ms_played"), F.lit(0)).alias("ms_played_filled"),
        F.when(F.col("device").isin("tv", "desktop"), "group_a").when(F.col("device") == "phone", "group_b").otherwise("other").alias("device_group"),
        F.when(F.col("is_skipped"), "yes").otherwise("no").alias("is_skipped_label"),
    )
)

with_trends = (
    filtered_plays
    .select(
        "*",
        F.count(F.lit(1)).over(w_listener_id).alias("rows_in_listener_id"),
        F.max("ms_played_filled").over(w_is_skipped_by_ms_played_prio).alias("ms_played_filled_max_2_prior"),
        F.nth_value("played_at", 2).over(w_is_skipped_label_by_played_a).alias("second_played_at"),
        F.ntile(4).over(w_is_skipped_by_ms_played).alias("ms_played_quartile"),
        F.round(F.cume_dist().over(w_all_by_ms_played_filled_ties), 3).alias("ms_played_filled_cume_dist"),
    )
)

metrics = (
    with_trends
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_filled",
        "device_group",
        "is_skipped_label",
        "rows_in_listener_id",
        "ms_played_filled_max_2_prior",
        "second_played_at",
        F.max("rows_in_listener_id").over(w_track_id_device_group).alias("track_id_device_group_max_rows_in_listener_id"),
        F.size(F.collect_set("source").over(w_track_id_device_group)).alias("distinct_source_in_track_id_device_group"),
    )
)

ranked = (
    metrics
    .select(
        "*",
        F.nth_value("ms_played_filled", 2).over(w_all_by_played_at_full).alias("second_ms_played_filled"),
        F.ntile(5).over(w_all_by_played_at).alias("played_at_quintile"),
        F.count(F.lit(1)).over(w_track_id_is_skipped_label).alias("rows_in_track_id_is_skipped_label"),
    )
)

result = (
    ranked
    .groupBy("played_at", "distinct_source_in_track_id_device_group")
    .pivot("device", ["speaker", "phone", "tv", "desktop"])
    .agg(F.sum("ms_played_filled_max_2_prior"))
    .orderBy("played_at", "distinct_source_in_track_id_device_group")
)
