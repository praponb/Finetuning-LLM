from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_listener_id_track_id = Window.partitionBy("listener_id", "track_id")
w_is_skipped = Window.partitionBy("is_skipped")
w_device_listener_id_by_ms_pla = Window.partitionBy("device", "listener_id").orderBy("ms_played", "id")
w_all_by_ms_played_moving_1 = Window.orderBy("ms_played", "id").rowsBetween(-1, Window.currentRow)
w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_is_skipped_by_ms_played = Window.partitionBy("is_skipped").orderBy("ms_played", "id")
w_track_id_ms_played_tercile = Window.partitionBy("track_id", "ms_played_tercile")
w_all_by_ms_played_vs_is_skipp = Window.orderBy("ms_played_vs_is_skipped_avg", "id")
w_played_at_to_date_device_gro = Window.partitionBy(F.to_date("played_at"), "device_group").orderBy("ms_played_rank")
w_played_at_to_date_device_gro_2 = Window.partitionBy(F.to_date("played_at"), "device_group").orderBy("ms_played_rank", "id")
w_ms_played_quintile_is_skippe = Window.partitionBy("ms_played_quintile", "is_skipped")
w_device_ms_played_tercile_by = Window.partitionBy("device", "ms_played_tercile").orderBy(F.col("ms_played_avg_2").desc_nulls_first())

scoped = (
    df_plays
    .filter((F.col("listener_id").isNotNull()) & (F.col("source") == "search"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
    )
)

windowed = (
    scoped
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        F.count(F.lit(1)).over(w_listener_id_track_id).alias("rows_in_listener_id_track_id"),
        F.round(F.avg("ms_played").over(w_listener_id_track_id), 2).alias("listener_id_track_id_avg_ms_played"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_is_skipped), 2).alias("ms_played_vs_is_skipped_avg"),
        F.ntile(5).over(w_device_listener_id_by_ms_pla).alias("ms_played_quintile"),
        F.round(F.avg("ms_played").over(w_all_by_ms_played_moving_1), 2).alias("ms_played_avg_2"),
        F.ntile(3).over(w_all_by_ms_played).alias("ms_played_tercile"),
        F.rank().over(w_is_skipped_by_ms_played).alias("ms_played_rank"),
        F.round(F.percent_rank().over(w_is_skipped_by_ms_played), 3).alias("ms_played_percent_rank"),
        F.lead("played_at", 2).over(w_all_by_ms_played).alias("next2_played_at"),
    )
)

labeled = (
    windowed
    .withColumn("device_group", F.when(F.col("device").isin("phone", "car"), "group_a").when(F.col("device") == "desktop", "group_b").otherwise("other"))
)

sequenced = (
    labeled
    .select(
        "*",
        F.round(F.avg("ms_played").over(w_track_id_ms_played_tercile), 2).alias("track_id_ms_played_tercile_avg_ms_played"),
        F.lead("next2_played_at", 2).over(w_all_by_ms_played_vs_is_skipp).alias("next2_next2_played_at"),
        F.round(F.cume_dist().over(w_played_at_to_date_device_gro), 3).alias("ms_played_rank_cume_dist"),
        F.ntile(10).over(w_played_at_to_date_device_gro_2).alias("ms_played_rank_decile"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_ms_played_quintile_is_skippe), F.lit(0)), 4).alias("ms_played_share_of_ms_played_quintile_is_skipped"),
        F.rank().over(w_device_ms_played_tercile_by).alias("ms_played_avg_2_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "rows_in_listener_id_track_id",
        "listener_id_track_id_avg_ms_played",
        "ms_played_vs_is_skipped_avg",
        "ms_played_quintile",
        "ms_played_avg_2",
        "ms_played_tercile",
        "ms_played_rank",
        "next2_played_at",
        "device_group",
        "track_id_ms_played_tercile_avg_ms_played",
        "next2_next2_played_at",
        "ms_played_rank_cume_dist",
        "ms_played_rank_decile",
        "ms_played_share_of_ms_played_quintile_is_skipped",
        "ms_played_avg_2_rank",
        F.substring(F.trim(F.col("device")), 1, 4).alias("device_prefix"),
    )
    .orderBy(F.col("ms_played_tercile").asc_nulls_last())
)
