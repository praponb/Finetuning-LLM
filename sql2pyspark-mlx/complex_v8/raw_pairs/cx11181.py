from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_device_by_ms_played_distance = Window.partitionBy("device").orderBy("ms_played_distance", "id")
w_source_device = Window.partitionBy("source", "device")
w_device_by_ms_played_distance_2 = Window.partitionBy("device").orderBy("ms_played_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_listener_id_source_by_distin = Window.partitionBy("listener_id", "source").orderBy("distinct_listener_id_in_source_device", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_change_full = Window.orderBy("ms_played_change", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_distinct_listener_id_in_sour = Window.partitionBy("distinct_listener_id_in_source_device")

cleaned_plays = (
    df_plays
    .filter((F.col("track_id") <= 14) & (~F.col("is_skipped")))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.abs(F.col("ms_played") - 195566).alias("ms_played_distance"),
    )
)

with_activity = (
    cleaned_plays.alias("s")
    .join(df_plays.alias("p"), (F.col("p.listener_id") == F.col("s.listener_id")) & (F.col("p.source").isin("playlist", "album")) & (F.col("p.track_id") <= 17), "left_anti")
)

metrics = (
    with_activity
    .select(
        "*",
        (F.col("ms_played") - F.lag("ms_played").over(w_device_by_ms_played_distance)).alias("ms_played_change"),
        F.size(F.collect_set("listener_id").over(w_source_device)).alias("distinct_listener_id_in_source_device"),
        F.first("ms_played").over(w_device_by_ms_played_distance_2).alias("first_ms_played"),
        F.round(F.col("ms_played_distance") - F.avg("ms_played_distance").over(w_source_device), 2).alias("ms_played_distance_vs_source_device_avg"),
    )
)

windowed = (
    metrics
    .select(
        "*",
        F.first("ms_played", ignorenulls=True).over(w_listener_id_source_by_distin).alias("first_known_ms_played"),
        F.nth_value("first_ms_played", 2).over(w_all_by_ms_played_change_full).alias("second_first_ms_played"),
        F.round(F.avg("ms_played_change").over(w_distinct_listener_id_in_sour), 2).alias("distinct_listener_id_in_source_device_avg_ms_pla"),
        F.round(F.col("ms_played_distance") / F.nullif(F.sum("ms_played_distance").over(w_distinct_listener_id_in_sour), F.lit(0)), 4).alias("ms_played_distance_share_of_distinct_listener_id"),
        F.round(F.col("first_ms_played") / F.nullif(F.sum("first_ms_played").over(w_distinct_listener_id_in_sour), F.lit(0)), 4).alias("first_ms_played_share_of_distinct_listener_id_in"),
    )
)

result = (
    windowed
    .select(
        "id",
        "listener_id",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        "ms_played_change",
        "first_ms_played",
        "ms_played_distance_vs_source_device_avg",
        "first_known_ms_played",
        "second_first_ms_played",
        "distinct_listener_id_in_source_device_avg_ms_pla",
        "ms_played_distance_share_of_distinct_listener_id",
        "first_ms_played_share_of_distinct_listener_id_in",
        F.substring(F.trim(F.col("source")), 1, 4).alias("source_prefix"),
        F.when(F.col("source").isin("playlist", "search"), "group_a").when(F.col("source") == "album", "group_b").otherwise("other").alias("source_group"),
    )
    .orderBy(F.col("source").desc_nulls_first())
)
