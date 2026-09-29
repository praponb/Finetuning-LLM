from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_ms_played = Window.orderBy("ms_played", "id")
w_listener_id = Window.partitionBy("listener_id")
w_all_by_played_at = Window.orderBy(F.col("played_at").desc(), "id")
w_listener_id_device_by_ms_pla = Window.partitionBy("listener_id", "device").orderBy("ms_played", "id")
w_is_skipped_track_id_by_ms_pl = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_by_played_at = Window.partitionBy("is_skipped").orderBy("played_at", "id")
w_listener_id_device_group = Window.partitionBy("listener_id", "device_group")
w_listener_id_played_at_year_b = Window.partitionBy("listener_id", F.year("played_at")).orderBy("played_at_day", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_is_skipped_track_id_by_ms_pl_2 = Window.partitionBy("is_skipped", "track_id").orderBy(F.col("ms_played").desc(), "id")

prepared = (
    df_plays
    .filter(~F.col("source").isin("playlist", "radio"))
    .select(
        "id",
        "listener_id",
        "track_id",
        "played_at",
        "ms_played",
        "device",
        "is_skipped",
        "source",
        F.when(F.col("device").isin("speaker", "tv"), "group_a").when(F.col("device") == "car", "group_b").otherwise("other").alias("device_group"),
        F.date_trunc("day", F.col("played_at")).alias("played_at_day"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.lead("played_at").over(w_all_by_ms_played).alias("next_played_at"),
        F.round(F.col("ms_played") - F.avg("ms_played").over(w_listener_id), 2).alias("ms_played_vs_listener_id_avg"),
        F.lag("played_at_day").over(w_all_by_played_at).alias("prev_played_at_day"),
        F.round(F.cume_dist().over(w_listener_id_device_by_ms_pla), 3).alias("ms_played_cume_dist"),
        F.last("played_at").over(w_is_skipped_track_id_by_ms_pl).alias("last_played_at"),
        F.round(F.cume_dist().over(w_is_skipped_by_played_at), 3).alias("played_at_cume_dist"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_listener_id_device_group), F.lit(0)), 4).alias("ms_played_share_of_listener_id_device_group"),
        F.lag("played_at").over(w_is_skipped_by_played_at).alias("prev_played_at"),
        F.round(F.percent_rank().over(w_listener_id_device_by_ms_pla), 3).alias("ms_played_percent_rank"),
        F.max("ms_played").over(w_listener_id_device_group).alias("listener_id_device_group_max_ms_played"),
        F.first("played_at").over(w_listener_id_played_at_year_b).alias("first_played_at"),
        F.ntile(10).over(w_is_skipped_track_id_by_ms_pl_2).alias("ms_played_decile"),
    )
)

result = (
    windowed
    .select("first_played_at", "next_played_at", "source")
    .distinct()
    .orderBy(F.col("source").desc())
)
