from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_track_id = Window.partitionBy("track_id")
w_all_by_ms_played_running = Window.orderBy("ms_played", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_track_id_listener_id = Window.partitionBy("track_id", "listener_id")
w_source_by_ms_played_distance = Window.partitionBy("source").orderBy("ms_played_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_ms_played_distance_fu = Window.orderBy(F.col("ms_played_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_source_by_ms_played_distance_2 = Window.partitionBy("source").orderBy("ms_played_distance", "id")
w_track_id_by_ms_played = Window.partitionBy("track_id").orderBy("ms_played", "id")
w_all_by_ms_played_ties = Window.orderBy("ms_played")
w_ms_played_quintile_track_id = Window.partitionBy("ms_played_quintile", "track_id").orderBy("track_id_max_ms_played_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_ms_played_quintile_track_id_2 = Window.partitionBy("ms_played_quintile", "track_id").orderBy("track_id_max_ms_played_distance", "id")
w_track_id_ms_played_quintile = Window.partitionBy("track_id", "ms_played_quintile").orderBy(F.col("track_id_listener_id_avg_ms_played").desc_nulls_first(), "id").rowsBetween(-6, Window.currentRow)
w_source_ms_played_quintile_by = Window.partitionBy("source", "ms_played_quintile").orderBy("distinct_device_in_track_id_listener_id", "id")
w_track_id_ms_played_quintile_2 = Window.partitionBy("track_id", "ms_played_quintile").orderBy(F.col("track_id_listener_id_avg_ms_played").desc_nulls_first(), "id")

filtered_plays = (
    df_plays
    .filter((F.col("listener_id") <= 9) & (F.month(F.col("played_at")).isin(1, 3)))
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        F.date_format(F.col("played_at"), "yyyy-MM").alias("played_at_label"),
        F.abs(F.col("ms_played") - 157900).alias("ms_played_distance"),
    )
)

windowed = (
    filtered_plays
    .select(
        "id",
        "listener_id",
        "track_id",
        "ms_played",
        "device",
        "source",
        "played_at_label",
        "ms_played_distance",
        F.sum("ms_played").over(w_track_id).alias("track_id_sum_ms_played"),
        F.first("ms_played", ignorenulls=True).over(w_all_by_ms_played_running).alias("first_known_ms_played"),
        F.size(F.collect_set("source").over(w_track_id_listener_id)).alias("distinct_source_in_track_id_listener_id"),
        F.first("ms_played_distance", ignorenulls=True).over(w_source_by_ms_played_distance).alias("first_known_ms_played_distance"),
        F.first("ms_played").over(w_all_by_ms_played_distance_fu).alias("first_ms_played"),
        F.sum("ms_played").over(w_all_by_ms_played_running).alias("running_ms_played"),
        F.max("ms_played_distance").over(w_track_id).alias("track_id_max_ms_played_distance"),
        F.dense_rank().over(w_source_by_ms_played_distance_2).alias("ms_played_distance_dense_rank"),
        F.round(F.col("ms_played") / F.nullif(F.sum("ms_played").over(w_track_id_listener_id), F.lit(0)), 4).alias("ms_played_share_of_track_id_listener_id"),
        F.ntile(5).over(w_track_id_by_ms_played).alias("ms_played_quintile"),
        F.round(F.percent_rank().over(w_all_by_ms_played_ties), 3).alias("ms_played_percent_rank"),
        F.round(F.avg("ms_played").over(w_track_id_listener_id), 2).alias("track_id_listener_id_avg_ms_played"),
        F.size(F.collect_set("listener_id").over(w_track_id)).alias("distinct_listener_id_in_track_id"),
        F.size(F.collect_set("device").over(w_track_id_listener_id)).alias("distinct_device_in_track_id_listener_id"),
        F.min("ms_played_distance").over(w_track_id_listener_id).alias("track_id_listener_id_min_ms_played_distance"),
    )
)

sequenced = (
    windowed
    .select(
        "*",
        F.last("first_ms_played").over(w_ms_played_quintile_track_id).alias("last_first_ms_played"),
        F.rank().over(w_ms_played_quintile_track_id_2).alias("track_id_max_ms_played_distance_rank"),
        F.max("ms_played").over(w_track_id_ms_played_quintile).alias("ms_played_max_7"),
        F.lead("ms_played_percent_rank").over(w_source_ms_played_quintile_by).alias("next_ms_played_percent_rank"),
        (F.col("track_id_sum_ms_played") - F.lag("track_id_sum_ms_played").over(w_track_id_ms_played_quintile_2)).alias("track_id_sum_ms_played_change"),
    )
)

result = (
    sequenced
    .filter(F.col("ms_played_distance") > 140000)
    .select("distinct_source_in_track_id_listener_id", "first_ms_played", "device")
    .distinct()
    .orderBy(
        F.col("distinct_source_in_track_id_listener_id").desc(),
        F.col("device").desc(),
    )
)
